#!/usr/bin/perl -w

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
use Params::Util qw(
    _NUMBER
);
use Time::HiRes ();

use Redis::CappedCollection qw(
    $NAMESPACE
);
use Redis::CappedCollection::Test::Utils qw(
    get_redis
    verify_redis
);

# -- Global variables
my $uuid = new Data::UUID;
my (
    $COLLECTION,
    $ERROR_MSG,
    $REDIS,
    $REDIS_SERVER,
    $STATUS_KEY,
    $PORT,
    $MAXMEMORY,
    $MEMORY_RESERVE,
    $MAX_DATA_BYTES_AVAILABLE,
);

( $REDIS_SERVER, $ERROR_MSG, $PORT ) = verify_redis();

SKIP: {
    diag $ERROR_MSG if $ERROR_MSG;
    skip( $ERROR_MSG, 1 ) if $ERROR_MSG;

    $MAXMEMORY = 1_000_000;

    # $min_cleanup_bytes > 0 && $min_cleanup_items == 0 
    test_insert(
        'min_cleanup_bytes' => 10_000,
        'min_cleanup_items' => 0,
        data_length         => 200,
    );

    # $min_cleanup_bytes == 0 && $min_cleanup_items > 0
    test_insert(
        'min_cleanup_bytes' => 0,
        'min_cleanup_items' => 10,
        data_length         => 200,
    );

    # $min_cleanup_bytes > 0 && $min_cleanup_items > 0 && $min_cleanup_bytes > $min_cleanup_items * $data_length
    test_insert(
        'min_cleanup_bytes' => 10_000,
        'min_cleanup_items' => 10,
        data_length         => 200,
    );

    # $min_cleanup_bytes > 0 && $min_cleanup_items > 0 && $min_cleanup_bytes > $min_cleanup_items * $data_length
    test_insert(
        'min_cleanup_bytes' => 1_000,
        'min_cleanup_items' => 10,
        data_length         => 200,
    );

    # $min_cleanup_bytes == 0 && $min_cleanup_items == 0
    test_insert(
        'min_cleanup_bytes' => 0,
        'min_cleanup_items' => 0,
        data_length         => 200,
    );
}

exit;

sub test_insert {
    my %args = @_;

    my $min_cleanup_bytes   = $args{min_cleanup_bytes};
    my $min_cleanup_items   = $args{min_cleanup_items};
    my $data_length         = $args{data_length};

    new_connection(
        'min_cleanup_bytes' => $min_cleanup_bytes,
        'min_cleanup_items' => $min_cleanup_items,
    );
    $MEMORY_RESERVE = $COLLECTION->memory_reserve;
    $MAX_DATA_BYTES_AVAILABLE = max_data_bytes_available();

    my $stuff = '*' x $data_length;
    my $list_id = 'Some list_id';

    my (
        $BEFORE_info,
        $AFTER_info,
        $BEFORE_last_cleanup_bytes,
        $AFTER_last_cleanup_bytes,
        $BEFORE_used_memory,
        $AFTER_used_memory,
    );

    my $inserts = 0;
    while ( 1 ) {
        my $data_time = Time::HiRes::time;
        my $data_id = $data_time;

        $BEFORE_info = $COLLECTION->collection_info;
        $BEFORE_last_cleanup_bytes = $COLLECTION->_call_redis( "HGET", $STATUS_KEY, 'last_cleanup_bytes'  );
        $BEFORE_used_memory = get_used_memory();

        my ( undef, $INSIDE_cleanings, $INSIDE_used_memory, $INSIDE_bytes_deleted ) = $COLLECTION->insert( $list_id, $data_id, $stuff, $data_time );
        ++$inserts;
        my $expected_used_memory = $INSIDE_used_memory + $data_length;
        my $total_bytes_added = $inserts * $data_length;

        $AFTER_used_memory = get_used_memory();
        $AFTER_info = $COLLECTION->collection_info;
        $AFTER_last_cleanup_bytes = $COLLECTION->_call_redis( "HGET", $STATUS_KEY, 'last_cleanup_bytes'  );

        if ( $AFTER_info->{last_removed_time} ) {   # cleaning
            diag "min_cleanup_bytes = $min_cleanup_bytes, min_cleanup_items = $min_cleanup_items";
            pass "added $total_bytes_added bytes from $MAX_DATA_BYTES_AVAILABLE available";
            pass "expected used memory = $expected_used_memory, used memory = $BEFORE_used_memory -> $AFTER_used_memory";
            ok $expected_used_memory > $MAX_DATA_BYTES_AVAILABLE, 'excess data';
            ok $AFTER_info->{last_removed_time} >= $BEFORE_info->{last_removed_time}, 'new last_removed_time';
            ok $AFTER_info->{items} < $AFTER_info->{items} + 1, 'data cleaned (items)';

            pass "INSIDE_cleanings = $INSIDE_cleanings";
            if (
                       ( $min_cleanup_bytes > 0 && $min_cleanup_items == 0 )
                    || ( $min_cleanup_bytes > 0 && $min_cleanup_items > 0 && $min_cleanup_bytes > $min_cleanup_items * $data_length )
                ) {
                is $INSIDE_cleanings,
                      ( $min_cleanup_bytes / $data_length )
                    + ( ( $min_cleanup_bytes % $data_length ) ? 1 : 0 ),
                    'OK cleanings'
                ;
                ok $AFTER_last_cleanup_bytes > 0, 'data cleaned (bytes)';
                is $AFTER_last_cleanup_bytes, $min_cleanup_bytes, 'bigger $min_cleanup_bytes';
            } elsif (
                       ( $min_cleanup_bytes == 0 && $min_cleanup_items > 0 )
                    || ( $min_cleanup_bytes > 0 && $min_cleanup_items > 0 && $min_cleanup_bytes < $min_cleanup_items * $data_length )
                ) {
                is $INSIDE_cleanings, $min_cleanup_items, 'OK cleanings';
                ok $AFTER_last_cleanup_bytes > 0, 'data cleaned (bytes)';
                is $AFTER_last_cleanup_bytes, $min_cleanup_items * $data_length, 'bigger $min_cleanup_items';
            } elsif ( $min_cleanup_bytes == 0 && $min_cleanup_items == 0 ) {
                # all data have the same size
                ok $INSIDE_cleanings >= 0, 'OK cleaning and insert';
                is $AFTER_last_cleanup_bytes, $data_length * $INSIDE_cleanings, 'data cleaned for new data';
            }

            last;
        } else {
            if ( $inserts % 100 == 0 ) {
                pass "added $total_bytes_added bytes from $MAX_DATA_BYTES_AVAILABLE available";
                pass "expected used memory = $expected_used_memory, used memory = $BEFORE_used_memory -> $AFTER_used_memory";
                is $AFTER_info->{last_removed_time}, $BEFORE_info->{last_removed_time}, 'last_removed_time not changed';
                is $AFTER_info->{last_removed_time}, 0, 'no clearing (last_removed_time)';
                is $AFTER_last_cleanup_bytes, $BEFORE_last_cleanup_bytes, 'last_cleanup_bytes not changed';
                is $AFTER_last_cleanup_bytes, 0, 'no clearing (last_cleanup_bytes)';
                is $AFTER_info->{items}, $BEFORE_info->{items} + 1, "item added ($inserts)";
                is $AFTER_info->{items}, $inserts, 'inserts OK';
                pass "added $total_bytes_added bytes from $MAX_DATA_BYTES_AVAILABLE available";
                ok $expected_used_memory <= $MAX_DATA_BYTES_AVAILABLE, 'no excess data';
            }
        }
    }

    # insert after cleaning
    my $data_time = Time::HiRes::time;
    my $data_id = $data_time;

    my ( undef, $INSIDE_cleanings, $INSIDE_used_memory, $INSIDE_bytes_deleted ) = $COLLECTION->insert( $list_id, $data_id, $stuff, $data_time );
    my $expected_used_memory = $INSIDE_used_memory + ( $min_cleanup_bytes == 0 && $min_cleanup_items == 0 ) ? 0 : $data_length;

    my $new_AFTER_used_memory = get_used_memory();
    my $new_AFTER_info = $COLLECTION->collection_info;
    my $new_AFTER_last_cleanup_bytes = $COLLECTION->_call_redis( "HGET", $STATUS_KEY, 'last_cleanup_bytes'  );

    pass "expected used memory = $expected_used_memory, used memory = $AFTER_used_memory -> $new_AFTER_used_memory";
    if ( $min_cleanup_bytes == 0 && $min_cleanup_items == 0 ) {
        ok $INSIDE_cleanings >= 0, 'maybe cleaning';
        if ( $INSIDE_cleanings ) {
            ok $new_AFTER_info->{last_removed_time} > $AFTER_info->{last_removed_time}, 'last_removed_time changed';
            is $new_AFTER_info->{items}, $AFTER_info->{items} - $INSIDE_cleanings + 1, 'item deleted and added';
            is $new_AFTER_last_cleanup_bytes, $INSIDE_cleanings * $data_length, 'last_cleanup_bytes';
        } else {
            ok $new_AFTER_info->{last_removed_time} >= $AFTER_info->{last_removed_time}, 'last_removed_time changed';
            ok $new_AFTER_info->{items} >= $AFTER_info->{items}, 'item deleted and added';
            ok $new_AFTER_last_cleanup_bytes <= $AFTER_last_cleanup_bytes, 'last_cleanup_bytes';
        }
    } else {
        is $INSIDE_cleanings, 0, 'no cleanings';
        is $new_AFTER_info->{last_removed_time}, $AFTER_info->{last_removed_time}, 'last_removed_time not changed';
        is $new_AFTER_last_cleanup_bytes, $AFTER_last_cleanup_bytes, 'last_cleanup_bytes changed by data length';
        is $new_AFTER_info->{items}, $AFTER_info->{items} + 1, 'item added ones';
    }
    ok $expected_used_memory <= $MAX_DATA_BYTES_AVAILABLE, 'no excess data';
}

sub max_data_bytes_available {
    return( int( $MAXMEMORY / ( 1 + $MEMORY_RESERVE ) ) );
}

sub get_used_memory {
    my $redis_memory_info = $REDIS->info( 'memory' );
    return $redis_memory_info->{used_memory};
}

sub new_connection {
    my %args = @_;

    my $min_cleanup_bytes   = $args{min_cleanup_bytes};
    my $min_cleanup_items   = $args{min_cleanup_items};

    if ( $REDIS_SERVER ) {
        $REDIS_SERVER->stop;
        undef $REDIS_SERVER;
    }

    $PORT = Net::EmptyPort::empty_port( $PORT );
    ( $REDIS_SERVER, $ERROR_MSG ) = get_redis(
        conf => {
            port                => $PORT,
            'maxmemory-policy'  => 'noeviction',
            maxmemory           => $MAXMEMORY,
        },
    );
    skip( $ERROR_MSG, 1 ) unless $REDIS_SERVER;
    isa_ok( $REDIS_SERVER, 'Test::RedisServer' );

    $COLLECTION = Redis::CappedCollection->create(
        redis               => $REDIS_SERVER,
        name                => $uuid->create_str,
        'older_allowed'     => 1,
        'min_cleanup_bytes' => $min_cleanup_bytes,
        'min_cleanup_items' => $min_cleanup_items,
    );
    isa_ok( $COLLECTION, 'Redis::CappedCollection' );

    $REDIS = $COLLECTION->_redis;
    isa_ok( $REDIS, 'Redis' );

    $STATUS_KEY = "$NAMESPACE:S:".$COLLECTION->name;
    ok $COLLECTION->_call_redis( 'EXISTS', $STATUS_KEY ), 'status hash created';

    return;
}
