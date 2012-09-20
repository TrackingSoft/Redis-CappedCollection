#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

# NAME: Redis::CappedCollection benchmark

use bytes;
use Carp;
use Time::HiRes     qw( gettimeofday );
use Redis;
use List::Util      qw( min );
use Getopt::Long    qw( GetOptions );

use Redis::CappedCollection qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    ECOLLDELETED
    EREDIS
    EDATAIDEXISTS
    EOLDERTHANALLOWED
    );

# PRECONDITIONS ----------------------------------------------------------------

#-- declarations ---------------------------------------------------------------

use constant {
    PORTION_TIME    => 10,
    INFO_TIME       => 60,
    QUEUE_PATTERN   => NAMESPACE.':queue:*',
    VISITOR_ID_LEN  => 20,
    };

my $host            = DEFAULT_SERVER;
my $port            = DEFAULT_PORT;
my $empty_port;
my $server;
my $redis;
my $max_size        = 512 * 1024 * 1024;        # 512GB
my $size_garbage    = 0;
my $max_lists       = 2_000_000;
my $data_len        = 200;
my $run_time        = 0;
my @not_evenly      = ();
my $receive         = 0;

my %bench           = ();
my $help            = 0;
my $work_exit       = 0;
my $coll;
my $coll_name       = $$.'';
my $visitors        = 0;
my $rate            = 0;

my $final_message = 'The End!';

#-- setting up facilities

my $ret = GetOptions(
    'host=s'            => \$host,
    'port=i'            => \$port,
    'coll_name=s'       => \$coll_name,
    'max_size=i'        => \$max_size,
    'size_garbage=i'    => \$size_garbage,
    'data_len=i'        => \$data_len,
    'visitors=i'        => \$max_lists,
    'run_time=i'        => \$run_time,
    'not_evenly=i{,}'   => \@not_evenly,
    'rate=f'            => \$rate,
    'receive'           => \$receive,
    "help|?"            => \$help,
    );

if ( !$ret or $help )
{
    print <<'HELP';
Usage: $0 [--host="..."] [--port=...] [--coll_name="..."] [--max_size=...] [--size_garbage=...] [--data_len=...] [--visitors=...] [--run_time=...] [--not_evenly V1 A1 V2 A2 ...] [--receive] [--help]

Start a Redis client, connect to the Redis server, randomly inserts or receives data

Options:
    --help
        Display this help and exit.

    --host="..."
        The server should contain an IP address of Redis server.
        If the server is not provided, '127.0.0.1' is used as the default for the Redis server.
    --port=N
        The server port.
        Default 6379 (the default for the Redis server).
    --coll_name="..."
        The collection name.
        Default = pid.
        Be used together with '--receive'.
    --max_size=N
        The maximum size, in bytes, of the capped collection data.
        Default 512MB.
    --size_garbage=N
        The minimum size, in bytes, of the data to be released, if the size of the collection data after adding new data may exceed 'max_size'.
        Default 0 - additional data should not be released.
    --data_len=...
        Length of the unit recording data.
        Default 200 byte.
    --visitors=N
        The number of visitors (data lists) that must be created.
        Default 2_000_000.
    --run_time=...
        Maximum, in seconds, run time.
        Default 0 - the work is not limited.
    --rate=N
        Exponentially distributed data with RATE.
        Default 0 - no exponentially data distributed.
    --receive
        Receive data instead of writing.
HELP
    exit 1;
}

my $item = '*' x $data_len;
my $upper;
$upper = exponential( 0.99999 ) if $rate;

$| = 1;

#-- definition of the functions

$SIG{INT} = \&tsk;

sub tsk {
    $SIG{INT} = "IGNORE";
    $final_message = 'Execution is interrupted!';
    ++$work_exit;
    return;
}

sub client_info {
    my $redis_info = $redis->info;
    print "WARNING: Do not forget to manually delete the test data.\n";
    print '-' x 78, "\n";
    print "CLIENT INFO:\n",
        "server         $server",                       "\n",
        "redis_version  $redis_info->{redis_version}",  "\n",
        "arch_bits      $redis_info->{arch_bits}",      "\n",
        "VISITOR_ID_LEN ", VISITOR_ID_LEN,              "\n",
        "PORTION_TIME   ", PORTION_TIME,                "\n",
        "INFO_TIME      ", INFO_TIME,                   "\n",
        "coll_name      $coll_name",                    "\n",
        "data_len       $data_len",                     "\n",
        "max_size       $max_size",                     "\n",
        "size_garbage   $size_garbage",                 "\n",
        "max_lists      $max_lists",                    "\n",
        "rate           $rate",                         "\n",
        "run_time       $run_time",                     "\n",
        '-' x 78,                                       "\n",
        ;
}

sub redis_info {
    my $redis_info = $redis->info;
    my $rss = $redis_info->{used_memory_rss};
    my $short_rss = ( $rss > 1024 * 1024 * 1024 ) ?
          $rss / ( 1024 * 1024 * 1024 )
        : $rss / ( 1024 * 1024 );
    my @status = $coll->validate;
    my $len = $status[0];
    my $short_len = ( $len > 1024 * 1024 * 1024 ) ?
          $len / ( 1024 * 1024 * 1024 )
        : $len / ( 1024 * 1024 );

    print
        "\n",
        '-' x 78,
        "\n",
        "data length             ", sprintf( "%.2f", $short_len ), ( $len > 1024 * 1024 * 1024 ) ? 'G' : 'M', "\n",
        "data lists              $status[1]\n",
        "data items              $status[2]\n",
        "mem_fragmentation_ratio $redis_info->{mem_fragmentation_ratio}\n",
        "used_memory_human       $redis_info->{used_memory_human}\n",
        "used_memory_rss         ", sprintf( "%.2f", $short_rss ), ( $rss > 1024 * 1024 * 1024 ) ? 'G' : 'M', "\n",
        "used_memory_peak_human  $redis_info->{used_memory_peak_human}\n",
        "used_memory_lua         $redis_info->{used_memory_lua}\n",
        "db0                     ", $redis_info->{db0} || "", "\n",
        '-' x 78,
        "\n",
        ;
}

# Send a request to Redis
sub call_redis {
    my $method      = shift;

    my @return = $redis->$method( @_ );

    return wantarray ? @return : $return[0];
}

sub exponential
{
    my $x = shift // rand;
    return log(1 - $x) / -$rate;
}

# Outputs random value in [0..LIMIT) range exponentially
# distributed with RATE. The higher is rate, the more skewed is
# the distribution (try rates from 0.1 to 3 to see how it changes).
sub get_exponentially_id {
# exponential distribution may theoretically produce big numbers
# (with low probability though) so we have to make an upper limit
# and treat all randoms greater than limit to be equal to it.

    my $r = exponential(); # exponentially distributed floating random
    $r = $upper if $r > $upper;
    return int( $r / $upper * ( $max_lists - 1 ) ); # convert to integer in (0..limit) range
}

# INSTRUCTIONS -----------------------------------------------------------------

$server = "$host:$port";
$redis = Redis->new( server => $server );
client_info();

if ( $receive and $coll_name eq $$.'' )
{
    print "There is no data to be read\n";
    goto THE_END;
}

$coll = Redis::CappedCollection->new(
    $redis,
    name            => $coll_name,
    $receive ? () : (
    size            => $max_size,
    size_garbage    => $size_garbage
    ),
    );

my $list_id;
my ( $secs, $count ) = ( 0, 0 );
my ( $time_before, $time_after );
my ( $start_time, $last_stats_reports_time, $last_info_reports_time );
$start_time = $last_stats_reports_time = $last_info_reports_time = time;
while ( !$work_exit )
{
    if ( $run_time ) { last if gettimeofday - $start_time > $run_time; }
    last if $work_exit;

    my $id          = ( !$receive and $rate ) ? get_exponentially_id() : int( rand $max_lists );
    $list_id        = sprintf( '%0'.VISITOR_ID_LEN.'d', $id );
    $time_before    = gettimeofday;
    my @ret         = $receive ? $coll->receive( $list_id ) : $coll->insert( $item, $list_id );
    $time_after     = gettimeofday;
    $secs += $time_after - $time_before;
    ++$count;

    my $time_from_stats = $time_after - $last_stats_reports_time;
    if ( $time_from_stats > PORTION_TIME )
    {
        print '[', scalar localtime, '] ',
            $receive ? 'reads' : 'inserts', ', ',
#            "$time_from_stats after stats, $secs operation sec, ",
            $secs ? sprintf( '%d', int( $count / $secs ) ) : 'N/A', ' op/sec ',
            ' ' x 5, "\r";

        if ( gettimeofday - $last_info_reports_time > INFO_TIME )
        {
            redis_info();
            $last_info_reports_time = gettimeofday;
        }

        $secs  = 0;
        $count = 0;
        $last_stats_reports_time = gettimeofday;
    }
}

# POSTCONDITIONS ---------------------------------------------------------------

print
    "\n",
    $final_message,
    "\n",
    ;
THE_END:
$redis->quit;

exit;
