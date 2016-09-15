#!/usr/bin/env perl

use strict;
use warnings;

use Config qw( %Config );
use Data::Dumper;
use JSON;
use LWP::UserAgent;
use Net::OpenSSH;
use Try::Tiny;
use XML::Simple;


if (!$ENV{SLACK_WEBHOOK_URL}) {
    warn "SLACK_WEBHOOK_URL is not set.\n";
}
if (!$ENV{SSH_HOST}) {
    die "SSH_HOST is not set.\n";
}
if (!$ENV{SSH_USER}) {
    die "SSH_USER is not set.\n";
}


$ENV{CMD_QSTAT} ||= "qstat";
$ENV{CMD_TRACEJOB} ||= "tracejob";
$ENV{DEBUG_JOB} ||= 0;
$ENV{DEBUG_SLACK} ||= 0;
$ENV{DEBUG_SSH} ||= 0;
$ENV{INTERVAL} ||= 5;
$ENV{SERVICE_NAME} ||= "torque-slack";
$ENV{SLACK_CHANNEL_JOB} ||= "";
$ENV{SLACK_CHANNEL_STATUS} ||= "";
$ENV{SLACK_TIMEOUT} ||= 5;
$ENV{STATUS_THRESHOLD_SERVER_DOWN} ||= 6;
$ENV{STATUS_THRESHOLD_SERVER_UP} ||= 1;
$ENV{STATUS_THRESHOLD_SERVICE_DOWN} ||= 1;
$ENV{STATUS_THRESHOLD_SERVICE_UP} ||= 1;
$ENV{SSH_PORT} ||= 22;
$ENV{SSH_TIMEOUT} ||= 5;
$ENV{TRACEJOB_N} ||= 7;
$ENV{FILE_USERS} ||= "/users.txt";
$ENV{FILE_JOB_STATES} ||= "/job_states.txt";

$| = 1; # Disable buffering in STDOUT

my %ssh_options = (
    batch_mode => 1,
    ctl_dir => "/dev", # requires tmpfs
    port => $ENV{SSH_PORT},
    timeout => $ENV{SSH_TIMEOUT},
    user => $ENV{SSH_USER},
);
if ($ENV{DEBUG_SSH}) {
    $Net::OpenSSH::debug = -1;
}
if ($ENV{SSH_GATEWAY}) {
    $ssh_options{gateway} = $ENV{SSH_GATEWAY};
}
if ($ENV{SSH_KEY_PATH}) {
    $ssh_options{key_path} = $ENV{SSH_KEY_PATH};
}
if ($ENV{SSH_MASTER_OPTS}) {
    $ssh_options{master_opts} = [ $ENV{SSH_MASTER_OPTS} ];
}


my @sig_name_by_num;
@sig_name_by_num[ split(' ', $Config{sig_num}) ] = split(' ', $Config{sig_name});

my $json = JSON->new();
my $ssh = undef;
my $ua = LWP::UserAgent->new(
    agent => $ENV{SERVICE_NAME},
    timeout => $ENV{SLACK_TIMEOUT},
    ssl_opts => { verify_hostname => 1 },
);
my $xs = XML::Simple->new(ForceArray => [ 'Job' ]);

my %status = (
    server => {
        count => 1,
        posted_value => "UP",
        post_message => "Server $ENV{SSH_HOST} is %s",
        value => "UP",
    },
    service => {
        count => 0,
        posted_value => "DOWN",
        post_message => "$ENV{SERVICE_NAME} for $ENV{SSH_HOST} is %s",
        value => "DOWN",
    },
);

my $job_state_map = load_map($ENV{FILE_JOB_STATES});
my $user_map = load_map($ENV{FILE_USERS});
my %jobs;


while (1) {
    try {
        # Fetch current jobs
        my %current_jobs;
        my $qstat_out = ssh_execute_command("$ENV{CMD_QSTAT} -x");
        if ($qstat_out) {
            my $obj = $xs->XMLin($qstat_out);
            %current_jobs = map { $_->{Job_Id} => $_ } @{$obj->{Job}};
        }

        # Submit / Update
        while (my($id, $job) = each %current_jobs) {
            if (exists $jobs{$id}) {
                if ($jobs{$id}{job_state} ne $job->{job_state}) {
                    print "Job state changed: id=$id, $jobs{$id}{job_state} -> $job->{job_state}\n";
                    update_job($job);
                }
            } else {
                # skip notification when reboot container
                if ($status{service}{value} eq "UP") {
                    update_job($job);
                }
                print "Job added: id=$id, state=$job->{job_state}\n";
            }
            $jobs{$id} = $job;
        }

        # Finish (collect resources_used information)
        my @finished;
        while (my($id, $job) = each %jobs) {
            next if exists $current_jobs{$id};

            my $trace_out = ssh_execute_command("$ENV{CMD_TRACEJOB} -w 1000 -n $ENV{TRACEJOB_N} $id");

            $job->{job_state} = "C"; # set as completed
            if ($trace_out =~ /Exit_status=(\S+)/) {
                $job->{exit_status} = $1;
            }
            while ($trace_out =~ /resources_used\.(\w+)=(\S+)/g) {
                $job->{resources_used}{$1} = $2;
            }

            update_job($job);

            print "Job removed: id=$id\n";
            push @finished, $id;
        }
        delete @jobs{@finished};

        update_status("server", "UP");
        update_status("service", "UP");
    } catch {
        warn $_;

        if (!$ssh || $ssh->error) {
            if ($ssh) {
                $ssh->disconnect(1);
                undef $ssh;
            }
            update_status("server", "DOWN");
        }

        # Initial error is often critial.
        exit 1 if $status{service}{value} eq "DOWN";
    };

    sleep $ENV{INTERVAL};
}

sub at_exit {
    $ssh->disconnect(1) if $ssh;
    update_status("service", "DOWN");
    exit 0;
}

sub update_status {
    my($what, $new_value) = @_;

    if (!exists $status{$what}) {
        die "unknown status key: $what";
    }
    if ($new_value ne "UP" && $new_value ne "DOWN") {
        die "unknown status value: $new_value";
    }

    if ($status{$what}{value} ne $new_value) {
        if ($what eq "service") {
            $SIG{INT} = $new_value eq "UP" ? \&at_exit : 'DEFAULT';
            $SIG{TERM} = $new_value eq "UP" ? \&at_exit : 'DEFAULT';
        }

        $status{$what}{value} = $new_value;
        $status{$what}{count} = 1;
    } else {
        $status{$what}{count}++;
    }

    if ($status{$what}{posted_value} ne $status{$what}{value}) {
        print "$what: $status{$what}{posted_value} -> $status{$what}{value} (count: $status{$what}{count})\n";

        my $env_key = uc sprintf "STATUS_THRESHOLD_%s_%s", $what, $status{$what}{value};
        if ($status{$what}{count} == $ENV{$env_key}) {
            my %payload = (
                text => sprintf($status{$what}{post_message}, $status{$what}{value}),
                username => $ENV{SERVICE_NAME},
            );
            if ($status{$what}{count} > 1) {
                $payload{text} .= " (with $status{$what}{count} retries)";
            }

            if ($ENV{SLACK_CHANNEL_STATUS}) {
                $payload{channel} = $ENV{SLACK_CHANNEL_STATUS};
            }
            post_to_slack(\%payload);

            $status{$what}{posted_value} = $status{$what}{value};
        }
    }
}

sub update_job {
    my($job) = @_;

    if ($ENV{DEBUG_JOB}) {
        print Dumper $job;
    }

    my $job_state_description = $job->{job_state};
    if (exists $job_state_map->{$job_state_description}) {
        $job_state_description = $job_state_map->{$job_state_description};
    }

    my $user = $job->{Job_Owner};
    $user =~ s/^(.+)\@$job->{server}$/$1/; # drop server
    if (exists $user_map->{$user}) {
        $user = '@'. $user_map->{$user};
    } else {
        $user = '@'. $user;
    }

    my %payload = (
        attachments => [
            {
                fallback => $job->{Job_Id},
                fields => [],
                ts => time(),
            },
        ],
        link_names => 1,
        text => "$user: $job_state_description $job->{Job_Name}",
        username => $ENV{SERVICE_NAME},
    );

    push @{$payload{attachments}[0]{fields}}, {
        title => "ID",
        value => $job->{Job_Id},
        short => JSON->true,
    }, {
        title => "Queue type",
        value => $job->{queue},
        short => JSON->true,
    };
    if ($job->{job_state} eq "R") {
        push @{$payload{attachments}[0]{fields}}, {
            title => "Hosts and cores",
            value => convert_exec_host($job->{exec_host}),
            short => JSON->false,
        };
    }
    if ($job->{job_state} eq "C") {
        push @{$payload{attachments}[0]{fields}}, {
            title => "Used CPU time",
            value => $job->{resources_used}{cput},
            short => JSON->true,
        }, {
            title => "Used wall time",
            value => $job->{resources_used}{walltime},
            short => JSON->true,
        }, {
            title => "Used physical memory",
            value => convert_mem_value($job->{resources_used}{mem}),
            short => JSON->true,
        }, {
            title => "Used virtual memory",
            value => convert_mem_value($job->{resources_used}{vmem}),
            short => JSON->true,
        };

        if ($job->{exit_status} > 256) {
            my $num = $job->{exit_status} - 256;
            my $value = $sig_name_by_num[$num] ? "$num (SIG$sig_name_by_num[$num])" : $num;
            push @{$payload{attachments}[0]{fields}}, {
                title => "Signal from PBS",
                value => $value,
                short => JSON->true,
            };
        } elsif ($job->{exit_status} > 128) {
            my $num = $job->{exit_status} - 128;
            my $value = $sig_name_by_num[$num] ? "$num (SIG$sig_name_by_num[$num])" : $num;
            push @{$payload{attachments}[0]{fields}}, {
                title => "Exit signal",
                value => $value,
                short => JSON->true,
            };
        } else {
            push @{$payload{attachments}[0]{fields}}, {
                title => "Exit status",
                value => $job->{exit_status},
                short => JSON->true,
            };
        };
        $payload{attachments}[0]{color} = $job->{exit_status} ? "warning" : "good";
    }

    if ($ENV{SLACK_CHANNEL_JOB}) {
        $payload{channel} = $ENV{SLACK_CHANNEL_JOB};
    }
    post_to_slack(\%payload);
}

sub post_to_slack {
    my $payload = shift;

    return unless exists $ENV{SLACK_WEBHOOK_URL};

    my $payload_json = $json->encode($payload);
    my $req = HTTP::Request->new(
        POST => $ENV{SLACK_WEBHOOK_URL},
    );
    $req->content_type('application/json');
    $req->content($payload_json);

    my $res = $ua->request($req);
    if (!$res->is_success) {
        print "Post failed.\n";
    }
    if (!$res->is_success || $ENV{DEBUG_SLACK}) {
        print "Slack payload: ". $payload_json. "\n";
        print "Slack response: ". $res->content. "\n";
    }
}

sub load_map {
    my($filename) = @_;

    my %map;
    if (-e $filename) {
        open my $fh, '<:utf8', $filename or die "$!: $filename\n";
        while (<$fh>) {
            chomp;
            next if $_ eq "" || /^#/;
            if (/^(\S+)\s+(.+)$/) {
                $map{$1} = $2;
            } else {
                warn "Fail to parse $filename: $_ (ignored)\n";
            }
        }
        close $fh;
    }
    return \%map;
}

sub ssh_execute_command {
    my($cmd) = @_;

    # Connect if disconnected
    if (!$ssh) {
        $ssh = Net::OpenSSH->new($ENV{SSH_HOST}, %ssh_options);
        if ($ssh->error) {
            die $ssh->error;
        }
    }

    # Execute a command
    my($out, $err) = $ssh->capture2($cmd);
    if ($?) {
        warn "Execute: $cmd\n";
        warn $err;
    }
    if ($ssh->error) {
        die $ssh->error;
    }

    return $out;
}

sub convert_exec_host {
    my($qstat_expression) = @_;

    my %num_cores;
    for my $process (split /\+/, $qstat_expression) {
        my $node = (split /\//, $process)[0];
        if (!exists $num_cores{$node}) {
            $num_cores{$node} = 0;
        }
        $num_cores{$node}++;
    }

    return join ", ", map {
        my $cores = $num_cores{$_} > 1 ? "cores" : "core";
        sprintf "%s (%d %s)", $_, $num_cores{$_}, $cores;
    } sort { $a cmp $b } keys %num_cores;
}

sub convert_mem_value {
    my($qstat_expression) = @_;

    my($value, $unit) = $qstat_expression =~ /^(\d+)(\D+)$/;
    while ($value =~ s/(\d)(\d\d\d(?:,|$))/$1,$2/) {}
    return "$value $unit";
}
