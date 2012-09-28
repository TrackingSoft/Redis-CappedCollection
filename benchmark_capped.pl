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
use List::Util      qw( min sum );
use Getopt::Long    qw( GetOptions );
use Data::Dumper;

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
    PORTION_TIME            => 10,
    INFO_TIME               => 60,
    QUEUE_PATTERN           => NAMESPACE.':queue:*',
    VISITOR_ID_LEN          => 20,
    THRESHOLD_STEP          => 1,
    LOST_PRODUCTIVITY       => 3,               # allowable percentage of lost productivity
    };

my $host                    = DEFAULT_SERVER;
my $port                    = DEFAULT_PORT;
my $empty_port;
my $server;
my $redis;
my $max_size                = 512 * 1024 * 1024;    # 512MB
my $advance_cleanup_bytes   = 0;
my $advance_cleanup_num     = 0;
my $max_lists               = 2_000_000;
my $data_len                = 200;
my $run_time                = 0;
my $receive                 = 0;
my $big_data_threshold      = 0;
my $find_optimal            = 0;
my $dump                    = 0;

my %bench                   = ();
my $help                    = 0;
my $work_exit               = 0;
my $coll;
my $coll_name               = $$.'';
my $visitors                = 0;
my $rate                    = 0;

my $final_message           = 'The End!';

#-- setting up facilities

my $ret = GetOptions(
    'host=s'                    => \$host,
    'port=i'                    => \$port,
    'coll_name=s'               => \$coll_name,
    'max_size=i'                => \$max_size,
    'advance_cleanup_bytes=i'   => \$advance_cleanup_bytes,
    'advance_cleanup_num=i'     => \$advance_cleanup_num,
    'data_len=i'                => \$data_len,
    'visitors=i'                => \$max_lists,
    'run_time=i'                => \$run_time,
    'rate=f'                    => \$rate,
    'find_optimal'              => \$find_optimal,
    'receive'                   => \$receive,
    'big_data_threshold=i'      => \$big_data_threshold,
    'dump=i'                    => \$dump,
    "help|?"                    => \$help,
    );

if ( !$ret or $help or ( $find_optimal and !$run_time ) )
{
    print <<'HELP';
Usage: $0 [--host="..."] [--port=...] [--coll_name="..."] [--max_size=...] [--advance_cleanup_bytes=...] [--advance_cleanup_num=...] [--data_len=...] [--visitors=...] [--run_time=...] [--rate=...] [--big_data_threshold=...] [--find_optimal] [--receive] [--dump=...] [--help]

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
    --advance_cleanup_bytes=N
        The minimum size, in bytes, of the data to be released, if the size of the collection data after adding new data may exceed 'max_size'.
        Default 0 - additional data should not be released.
    --advance_cleanup_num=N
        The amount of data that must be removed, if the size of the collection data after adding new data may exceed 'max_size'.
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
    --big_data_threshold=...
        The maximum number of items of data to store as a common hashes.
        Default 0 - storage are separate hashes and ordered lists.
    --rate=N
        Exponentially distributed data with RATE.
        Default 0 - no exponentially data distributed.
    --find_optimal
        Find the optimal value of 'big_data_threshold'.
        Permissible loss of performance of 3 percent.
        To perform the required set value of 'run_time'. This value will be used as a test duration of each mode.
        Testing is carried out separately for increasing values ​​of 'big_data_threshold', starting from zero.
        WARNING: Data collection are removed after each stage (to ensure the same test conditions).
    --dump=N
        Amount of data for which to perform dump.
        Default 0 - does not perform.
        WARNING: Before work removes all keys from the current database.
    --receive
        Receive data instead of writing.
HELP
    exit 1;
}

my $item = '*' x $data_len;
my $upper = 0;
$upper = exponential( 0.99999 ) if $rate;
my $no_line = 0;

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
    print "WARNING: Do not forget to manually delete the test data.\n" unless $find_optimal;
    print '-' x 78, "\n";
    print "CLIENT INFO:\n",
        "server                 $server",                       "\n",
        "redis_version          $redis_info->{redis_version}",  "\n",
        "arch_bits              $redis_info->{arch_bits}",      "\n",
        "VISITOR_ID_LEN         ", VISITOR_ID_LEN,              "\n",
        "PORTION_TIME           ", PORTION_TIME,                "\n",
        "INFO_TIME              ", INFO_TIME,                   "\n",
        "coll_name              $coll_name",                    "\n",
        "data_len               $data_len",                     "\n",
        "max_size               $max_size",                     "\n",
        "advance_cleanup_bytes  $advance_cleanup_bytes",        "\n",
        "advance_cleanup_num    $advance_cleanup_num",          "\n",
        "big_data_threshold     $big_data_threshold",           "\n",
        "max_lists              $max_lists",                    "\n",
        "rate                   $rate",                         "\n",
        "find_optimal           $find_optimal",                 "\n",
        "dump                   $dump",                         "\n",
        "run_time               $run_time",                     "\n",
        '-' x 78,                                               "\n",
        ;
}

sub redis_info {
    my $redis_info = $redis->info;
    my $rss = $redis_info->{used_memory_rss};
    my $short_rss = ( $rss > 1024 * 1024 * 1024 ) ?
          $rss / ( 1024 * 1024 * 1024 )
        : $rss / ( 1024 * 1024 );
    my $info = $coll->collection_info;
    my $len = $info->{length};
    my $short_len = ( $len > 1024 * 1024 * 1024 ) ?
          $len / ( 1024 * 1024 * 1024 )
        : $len / ( 1024 * 1024 );

    print
        "\n",
        '-' x 78,
        "\n",
        "data length             ", sprintf( "%.2f", $short_len ), ( $len > 1024 * 1024 * 1024 ) ? 'G' : 'M', "\n",
        "data lists              $info->{lists}\n",
        "data items              $info->{items}\n",
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

sub measurement_info {
    my $measurement    = shift;

    print
        sprintf( "THRESHOLD %3d, mean %.2f op/sec, mem %7s, rate has fallen by ",
            $measurement->{threshold},
            $measurement->{mean},
            $measurement->{used_memory},
            ),
        $measurement->{fallen} eq 'N/A' ? 'N/A' : sprintf( "%.2f%%", $measurement->{fallen} ),
        "\n";
}

sub _get_value {
    my $coll    = shift;
    my $key     = shift;
    my $type    = shift;

    return $coll->_call_redis( 'GET', $key )            if $type eq 'string';
    return $coll->_call_redis( 'LRANGE', $key, 0, -1 )  if $type eq 'list';
    return $coll->_call_redis( 'SMEMBERS', $key )       if $type eq 'set';

    if ( $type eq 'zset' ) {
        my %hash;
        my @zsets = $coll->_call_redis( 'ZRANGE', $key, 0, -1, 'WITHSCORES' );
        for ( my $loop = 0; $loop < scalar( @zsets ) / 2; $loop++ )
        {
            my $value = $zsets[ $loop * 2 ];
            my $score = $zsets[ ( $loop * 2 ) + 1 ];
            $hash{ $score } = $value;
        }
        return [ {%hash} ];
    }

    if ( $type eq 'hash' ) {
        my %hash;
        foreach my $item ( $coll->_call_redis( 'HKEYS', $key ) )
        {
            $hash{ $item } = $coll->_call_redis( 'HGET', $key, $item );
        }
        return {%hash};
    }
}

sub redis_dump {
    my $coll    = shift;

    my $redis_info = $redis->info;
    my %keys;

    printf( "used_memory %dB / %s\n", $redis_info->{used_memory}, $redis_info->{used_memory_human} );

    foreach my $key ( $coll->_call_redis( 'KEYS', '*' ) ) {
        my $type = $coll->_call_redis( 'TYPE', $key );
        my $show_name   = $key;
        my $encoding    = $coll->_call_redis( 'OBJECT', 'ENCODING', $key );
        my $refcount    = $coll->_call_redis( 'OBJECT', 'REFCOUNT', $key );
        $show_name     .= " ($type/$encoding: $refcount)";

        $keys{ $show_name } = _get_value( $coll, $key, $type );
    }
    return \%keys;
}

# INSTRUCTIONS -----------------------------------------------------------------

$server = "$host:$port";
$redis  = Redis->new( server => $server );

client_info();

if ( $receive and $coll_name eq $$.'' )
{
    print "There is no data to be read\n";
    goto THE_END;
}

my @measurements = ();
$big_data_threshold = 0 if $find_optimal;

do
{
    $redis->flushall if $dump;
    $coll = Redis::CappedCollection->new(
        $redis,
        name            => $coll_name,
        $receive ? () : (
            size                    => $max_size,
            advance_cleanup_bytes   => $advance_cleanup_bytes,
            advance_cleanup_num     => $advance_cleanup_num,
            big_data_threshold      => $big_data_threshold,
            ),
        );

    my $measurement = {
        threshold   => $big_data_threshold,
        speed       => [],
        mean        => 0,
        fallen      => 0,
        used_memory => '',
        };

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

        if ( $dump and $count >= $dump )
        {
            my $dump = redis_dump( $coll );
            $Data::Dumper::Sortkeys = 1;
            print Dumper( $dump );
            goto THE_END;
        }

        my $redis_info = $redis->info;
        $measurement->{used_memory} = $redis_info->{used_memory_human},

        my $time_from_stats = $time_after - $last_stats_reports_time;
        if ( $time_from_stats > PORTION_TIME )
        {
            my $speed = int( $count / $secs );
            push( @{$measurement->{speed}}, $speed )if $find_optimal;
            print '[', scalar localtime, '] ',
                $receive ? 'reads' : 'inserts', ', ',
                $secs ? sprintf( '%d', $speed ) : 'N/A', ' op/sec ',
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

    goto THE_FINISH if $work_exit;
    if ( $find_optimal )
    {

        $measurement->{mean}        = sum( @{$measurement->{speed}} ) / scalar @{$measurement->{speed}};
        push @measurements, $measurement;
        $measurement->{fallen}      = ( $measurements[0]->{mean} and $measurement->{mean} ) ? ( $measurements[0]->{mean} - $measurement->{mean} ) * 100 / $measurements[0]->{mean} : 'N/A';

        measurement_info( $measurement );
        print
            '-' x 78,
            "\n";

        $big_data_threshold += THRESHOLD_STEP if $find_optimal;
        $coll->drop_collection;

        goto THE_FINISH if $measurement->{fallen} > LOST_PRODUCTIVITY;
    }

} while $find_optimal;

# POSTCONDITIONS ---------------------------------------------------------------

THE_FINISH:

if ( $find_optimal )
{
    print "\n", '-' x 78, "\n";
    $no_line = 1;
    print "\nHISTORY:\n";
    foreach my $iteration ( @measurements )
    {
        measurement_info( $iteration );
    }
    $coll->drop_collection if $coll->name;
}

print
    "\n",
    $final_message,
    "\n",
    ;
THE_END:
$redis->quit;

exit;
