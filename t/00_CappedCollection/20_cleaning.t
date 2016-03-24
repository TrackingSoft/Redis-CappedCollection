#!/usr/bin/perl -w
#TODO: to develop tests
# - memory errors (working with ROLLBACK)
# - with maxmemory = 0
# - $E_NONEXISTENT_DATA_ID error


use 5.010;
use strict;
use warnings;

use lib qw(
    lib
    t/tlib
);

use Test::More;
plan 'no_plan';

BEGIN {
    eval 'use Test::RedisServer';               ## no critic
    plan skip_all => 'because Test::RedisServer required for testing' if $@;
}

BEGIN {
    eval 'use Net::EmptyPort';                  ## no critic
    plan skip_all => 'because Net::EmptyPort required for testing' if $@;
}

BEGIN {
    eval 'use Test::Exception';                 ## no critic
    plan skip_all => 'because Test::Exception required for testing' if $@;
}

BEGIN {
    eval 'use Test::NoWarnings';                ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

use Data::UUID;
use File::Spec ();
use JSON::XS ();
use Params::Util qw(
    _NUMBER
);
use Time::HiRes ();

use Redis::CappedCollection qw(
    $DEFAULT_PORT
    $DEFAULT_SERVER
    $E_MAXMEMORY_LIMIT
    $E_NONEXISTENT_DATA_ID
    $E_REDIS
    $NAMESPACE
);
use Redis::CappedCollection::Test::Utils qw(
    get_redis
    verify_redis
);

# -- Global variables
my $uuid = new Data::UUID;
my (
    $ADVANCE_CLEANUP_BYTES,
    $ADVANCE_CLEANUP_NUM,
    $COLLECTION,
    $COLLECTION_NAME,
    $ERROR_MSG,
    $JSON,
    $LAST_REDIS_USED_MEMORY,
    $MAXMEMORY,
    $MEMORY_RESERVE,
    $MEMORY_RESERVE_COEFFICIENT,
    $REDIS,
    $REDIS_LOG,
    $REDIS_SERVER,
    $STATUS_KEY,
    $QUEUE_KEY,
    $cleanings_performed,
    $debug_records,
    $inserts,
    $list_id,
    @operation_times,
    $stuff,
    $port,
);

( $REDIS_SERVER, $ERROR_MSG, $port ) = verify_redis();

SKIP: {
    diag $ERROR_MSG if $ERROR_MSG;
    skip( $ERROR_MSG, 1 ) if $ERROR_MSG;

sub new_connection {
    my ( $name, $maxmemory, $advance_cleanup_bytes, $advance_cleanup_num, $memory_reserve ) = @_;

    if ( $REDIS_SERVER ) {
        $REDIS_SERVER->stop;
        undef $REDIS_SERVER;
    }

    $port = Net::EmptyPort::empty_port( $port );
    ( $REDIS_SERVER, $ERROR_MSG ) = get_redis(
        conf => {
            port                => $port,
            'maxmemory-policy'  => 'noeviction',
            $maxmemory ? ( maxmemory => $maxmemory ) : ( maxmemory => 0 ),
        },
    );
    skip( $ERROR_MSG, 1 ) unless $REDIS_SERVER;
    isa_ok( $REDIS_SERVER, 'Test::RedisServer' );
    $REDIS_LOG = File::Spec->catfile( $REDIS_SERVER->tmpdir, 'redis-server.log' );

    $COLLECTION = Redis::CappedCollection->create(
        redis                   => $REDIS_SERVER,
        name                    => $uuid->create_str,
        'older_allowed'         => 1,
        $name                   ? ( name                    => $name )                  : (),
        $advance_cleanup_bytes  ? ( 'advance_cleanup_bytes' => $advance_cleanup_bytes ) : (),
        $advance_cleanup_num    ? ( 'advance_cleanup_num'   => $advance_cleanup_num   ) : (),
        $memory_reserve         ? ( memory_reserve          => $memory_reserve )        : (),
    );
    isa_ok( $COLLECTION, 'Redis::CappedCollection' );
    $COLLECTION_NAME        = $COLLECTION->name;
    $ADVANCE_CLEANUP_BYTES  = $COLLECTION->advance_cleanup_bytes;
    $ADVANCE_CLEANUP_NUM    = $COLLECTION->advance_cleanup_num;
    $MEMORY_RESERVE         = $COLLECTION->memory_reserve;

    $REDIS = $COLLECTION->_redis;
    isa_ok( $REDIS, 'Redis' );
    $REDIS->config_set( maxmemory => $maxmemory );
    $REDIS->config_set( 'maxmemory-policy' => 'noeviction' );

    $STATUS_KEY = "$NAMESPACE:S:$COLLECTION_NAME";
    $QUEUE_KEY  = "$NAMESPACE:Q:$COLLECTION_NAME";
    ok $COLLECTION->_call_redis( 'EXISTS', $STATUS_KEY ), 'status hash created';
    ok !$COLLECTION->_call_redis( 'EXISTS', $QUEUE_KEY ), 'queue list not created';

    $JSON = JSON::XS->new;

    return;
}

sub read_log {
    my ( $line, %debug_records );

    open( my $redis_log_fh, '<', $REDIS_LOG ) or die "cannot open < $REDIS_LOG: $!";
    while ( $line = <$redis_log_fh>) {
        chomp $line;
        if ( my ( $func_name, $json_text ) = $line =~ /[^*]+\*\s+(\w+):\s+(.+)/ ) {
            my $result = $JSON->decode( $json_text );
            my $debug_id = delete $result->{_DEBUG_ID};
            $debug_records{ $func_name }->{ $debug_id } = []
                unless exists $debug_records{ $func_name }->{ $debug_id } ;
            my $debug_measurements = $debug_records{ $func_name }->{ $debug_id };
            push @$debug_measurements, $result;
        }
    }
    close $redis_log_fh;

    return \%debug_records;
}

sub verifying {
    my ( $tested_function ) = @_;

    $debug_records = read_log();
#use Data::Dumper;
#$Data::Dumper::Sortkeys = 1;
#say STDERR '# DUMP: ', Data::Dumper->Dump( [ $debug_records ], [ 'debug_records' ] );

    $cleanings_performed = 0;
    foreach my $operation_id ( sort keys %{ $debug_records->{ $tested_function } } ) {
        my (
            $calculated_cleaning_needed,
            $cleaning_performed,
            $enough_memory_cleaning_needed,
            $exists_before_cleanings,
            $items_deleted,
        );

        foreach my $step ( @{ $debug_records->{ $tested_function }->{ $operation_id } } ) {
            if ( $step->{_STEP} eq 'Cleaning needed or not?' ) {
                $enough_memory_cleaning_needed = $step->{enough_memory_cleaning_needed};
            } elsif ( $step->{_STEP} eq 'Before cleanings' ) {
                $exists_before_cleanings = 1;
                $calculated_cleaning_needed =
                       $step->{coll_items} > 0
                    && (
                        (
                            (
                                   $step->{is_forced_cleaning}
                                && $step->{enough_memory_cleaning_needed} > 0
                             )
                            || (
                                   $step->{LAST_REDIS_USED_MEMORY} * $step->{MEMORY_RESERVE_COEFFICIENT} >= $step->{MAXMEMORY}
                                || $step->{enough_memory_cleaning_needed} > 0
                            )
                        )
                        || (
                               $step->{advance_remaining_iterations} > 0
                            || (
                                   $step->{advance_cleanup_bytes} > 0
                                && $step->{advance_bytes_deleted} < $step->{advance_cleanup_bytes}
                            )
                        )
                        || $step->{enough_memory_cleaning_needed} > 0
                    );
            } elsif ( $step->{_STEP} eq 'Before real cleaning' ) {
                ok $step->{excess_data_id} eq shift( @operation_times ), 'excess data time OK';
            } elsif ( $step->{_STEP} eq 'After real cleaning' ) {
                ok !$COLLECTION->_call_redis( 'HEXISTS', $step->{excess_data_key}, $step->{excess_data_id} ), 'excess data deleted';
                $cleaning_performed = 1;
                ++$cleanings_performed;
                ++$items_deleted;
            } elsif ( $step->{_STEP} eq 'Cleaning finished' ) {
                ok $step->{total_items_deleted} == $items_deleted,                      'total items deleted OK';
                ok $step->{advance_remaining_iterations} <= 0,                          'advance remaining iterations OK';
                ok $step->{is_advance_cleanup_bytes_worked_out} == 1,                   'advance cleanup bytes worked out';
                ok $step->{is_enough_memory} == 1,                                      'enough_memory';
                ok $step->{is_memory_cleared} == 1,                                     'memory_cleared';
                ok $step->{total_bytes_deleted} > $step->{advance_cleanup_bytes},       'total bytes deleted OK';
            }

            $LAST_REDIS_USED_MEMORY     = $step->{LAST_REDIS_USED_MEMORY};
            $MEMORY_RESERVE_COEFFICIENT = $step->{MEMORY_RESERVE_COEFFICIENT};
            $MAXMEMORY                  = $step->{MAXMEMORY};
        }

        if ( $cleaning_performed ) {
            ok $exists_before_cleanings,        'exists before cleanings';
            ok $calculated_cleaning_needed,     'cleaning needed';
        } else {
            ok !$exists_before_cleanings || !$calculated_cleaning_needed, 'not exists before cleanings';
        }
    }
    ok $cleanings_performed, 'cleanings performed';
    ok $LAST_REDIS_USED_MEMORY * $MEMORY_RESERVE_COEFFICIENT < $MAXMEMORY, 'cleaning OK';
}

#-- Insert ---------------------------------------------------------------------

my $prev_time = 0;
my $time_grows = 0;
foreach my $current_advance_cleanup_bytes ( 0, 100, 10_000 ) {
    foreach my $current_advance_cleanup_num ( 0, 100, 10_000 ) {
        new_connection(
            undef,      # name
#TODO:
#            0,          # maxmemory
            1_000_000,  # maxmemory
            $current_advance_cleanup_bytes,
            $current_advance_cleanup_num,
        );

        $stuff = '*' x 1_000;
        $list_id = 'Some list_id';
        @operation_times = ();
        $inserts = 1_000;
        my $last_removed_time = $COLLECTION->collection_info->{last_removed_time};
        ok defined( _NUMBER( $last_removed_time ) ) && $last_removed_time == 0, 'OK last_removed_time before';
        for ( 1 .. $inserts ) {
            my $data_time = Time::HiRes::time;
            my $data_id = $data_time;
            push @operation_times, $data_time;
            $COLLECTION->_DEBUG( $data_time );
            $COLLECTION->insert( $list_id, $data_id, $stuff, $data_time );
        }
        $last_removed_time = $COLLECTION->collection_info->{last_removed_time};
        ok $last_removed_time > 0, 'OK last_removed_time after';
        ok $last_removed_time >= $prev_time, 'OK last_removed_time';
        ++$time_grows if $last_removed_time > $prev_time;
        $prev_time = $last_removed_time;
        verifying( 'insert' );
    }
}
ok $time_grows, 'last_removed_time grows';

#-- Update ---------------------------------------------------------------------

$MAXMEMORY = 2_000_000;
foreach my $current_advance_cleanup_bytes ( 0, 100, 10_000 ) {
    foreach my $current_advance_cleanup_num ( 0, 100, 10_000 ) {
        new_connection(
            undef,      # name
#TODO:
#            0,          # maxmemory
            $MAXMEMORY, # maxmemory
            $current_advance_cleanup_bytes,
            $current_advance_cleanup_num,
        );

        @operation_times = ();
        my %data_lists;
        my $inserted_data_size = 0;
        while ( $inserted_data_size < $MAXMEMORY ) {
            my $data_time = Time::HiRes::time;
            my $data_id = $data_time;
            push @operation_times, $data_time;
            $COLLECTION->_DEBUG( $data_time );
            $stuff = '*' x ( int( rand( 1_000 ) ) + 2 );
            $inserted_data_size += length( $stuff );
            $list_id = int( rand( 1_000 ) ) + 1;
            $data_lists{ $data_id } = $list_id;
            $COLLECTION->_DEBUG( Time::HiRes::time );
            $COLLECTION->insert( $list_id, $data_id, $stuff, $data_time );
        }
        verifying( 'insert' );

        $COLLECTION->_DEBUG( Time::HiRes::time );
        my $data_id  = $operation_times[ -100 ];    # cause $data_id == $data_time
        $list_id = $data_lists{ $data_id };
        $stuff = '@' x 100_000;
        $COLLECTION->update( $list_id, $data_id, $stuff );
        verifying( 'update' );

        #-- cleaning himself
        $data_id = $operation_times[0];
        $list_id = $data_lists{ $data_id };
        $COLLECTION->_DEBUG( Time::HiRes::time );
        eval { $COLLECTION->update( $list_id, $data_id, $stuff ) };
        my $error = $@;
        ok $error, 'exception';
        is $COLLECTION->last_errorcode, $E_REDIS, 'E_REDIS';
        my $error_msg = $Redis::CappedCollection::ERROR{ $E_MAXMEMORY_LIMIT };
        like( $error, qr/$error_msg/, 'E_MAXMEMORY_LIMIT' );
        note '$@: ', $error;

        #-- $E_NONEXISTENT_DATA_ID
        $data_id = 99999999;
        $COLLECTION->_DEBUG( Time::HiRes::time );
        ok !$COLLECTION->update( $list_id, $data_id, $stuff ), 'data not exists';
#TODO:
# - When DATA_KEY not exists
# - When data cleaned
    }
}

#-- ROLLBACK ---------------------------------------------------------------------
#$MAXMEMORY = 2_000_000;
#new_connection(
#    undef,      # name
##TODO:
##    0,          # maxmemory
#    $MAXMEMORY, # maxmemory
#);
#
#@operation_times = ();
#my %data_lists;
#my $inserted_data_size = 0;
#while ( $inserted_data_size < $MAXMEMORY ) {
#    my $data_time = Time::HiRes::time;
#    my $data_id = $data_time;
#    push @operation_times, $data_time;
#    $COLLECTION->_DEBUG( $data_time );
#    $stuff = '*' x ( int( rand( 1_000 ) ) + 2 );
#    $inserted_data_size += length( $stuff );
#    $list_id = int( rand( 1_000 ) ) + 1;
#    $data_lists{ $data_id } = $list_id;
#    $COLLECTION->insert( $list_id, $data_id, $stuff, $data_time );
#}
#verifying( 'insert' );
#
#$COLLECTION->_DEBUG( Time::HiRes::time );
#my $data_id  = $operation_times[ -100 ];    # cause $data_id == $data_time
#$list_id = $data_lists{ $data_id };
##TODO:
## - how to achieve the desired error?
##   - in _DEBUG mode to cause error?
## - incorrect or unclear we are working with 'maxmemory-policy noeviction'
#$stuff = '@' x ( int( $MAXMEMORY / $MEMORY_RESERVE_COEFFICIENT ) - 1 );
#$COLLECTION->update( $list_id, $data_id, $stuff );
#verifying( 'update' );

}