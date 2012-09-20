#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::RedisServer";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Test::TCP";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

use Time::HiRes qw( gettimeofday );
use Redis::CappedCollection qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE
    );

use constant {
    TEST_SECS       => 3,
    VISITOR_ID_LEN  => 20,
    DATA_LEN        => 17,
    MAX_LISTS       => 2_000,
    MAX_SIZE        => 35_000,
    SIZE_GARBAGE    => 501,
    };

my $redis;
my $real_redis;

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg and !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

sub new_connect {
    my $size_garbage    = shift;

    $redis = Test::RedisServer->new( conf =>
        {
            port                => empty_port(),
            maxmemory           => 0,
            "maxmemory-policy"  => 'noeviction',
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    my $coll = Redis::CappedCollection->new(
        $redis,
        size            => MAX_SIZE,
        size_garbage    => $size_garbage,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    return $coll;
}

sub test_insert {
    my $size_garbage    = shift;

    my $data_num        = 0;
    my $data_len        = 0;
    my @data            = ();
    my @real_data       = ();

    my $coll = new_connect( $size_garbage );
    my $start_time = gettimeofday;
    while ( gettimeofday - $start_time < TEST_SECS )
    {
        my $list_id = sprintf( '%0'.VISITOR_ID_LEN.'d', int( rand MAX_LISTS ) );
        push @data, $data_num;
        $coll->insert(
            sprintf( '%0'.DATA_LEN.'d',         $data_num++ ),
            sprintf( '%0'.VISITOR_ID_LEN.'d',   int( rand MAX_LISTS ) ),
            );

        while ( scalar( @data ) * DATA_LEN > $coll->size )
        {
            while ( scalar( @data ) * DATA_LEN > $coll->size - $coll->size_garbage )
            {
                shift @data;
            }
        }
    }

    @real_data = ();
    while ( my ( $list_id, $data ) = $coll->pop_oldest )
    {
        push @real_data, $data + 0;
    }

    is "@real_data", "@data", 'everything is working properly('.( scalar @data ).' The remaining elements)';

}

test_insert( 0 );                               #-- only MAX_SIZE
test_insert( SIZE_GARBAGE );                    #-- MAX_SIZE and SIZE_GARBAGE

}

