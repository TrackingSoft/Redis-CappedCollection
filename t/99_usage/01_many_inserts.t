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
    plan skip_all => "because Test::TCP required for testing" if $@;
}

use Time::HiRes qw( gettimeofday );
use Redis::CappedCollection qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE
    );

use constant {
    TEST_SECS               => 3,                       # recommend 30
    VISITOR_ID_LEN          => 20,
    DATA_LEN                => 17,
    MAX_LISTS               => 2_000,
    MAX_SIZE                => 35_000,
    advance_cleanup_bytes   => 501,
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
    my $advance_cleanup_bytes   = shift;
    my $big_data_threshold      = shift || 0;

    $redis = Test::RedisServer->new( conf =>
        {
            port                => empty_port(),
            maxmemory           => 0,
            "maxmemory-policy"  => 'noeviction',
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    my $coll = Redis::CappedCollection->new(
        $redis,
        size                    => MAX_SIZE,
        advance_cleanup_bytes   => $advance_cleanup_bytes,
        big_data_threshold      => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    return $coll;
}

sub test_insert {
    my $advance_cleanup_bytes   = shift;
    my $big_data_threshold      = shift || 0;

    my $data_num                = 0;
    my $data_len                = 0;
    my @data                    = ();
    my @real_data               = ();

    my $coll = new_connect( $advance_cleanup_bytes, $big_data_threshold );
    my $start_time = gettimeofday;
    while ( gettimeofday - $start_time < TEST_SECS )
    {
        my $list_id = sprintf( '%0'.VISITOR_ID_LEN.'d', int( rand MAX_LISTS ) );
        push @data, $data_num;
        $coll->insert(
            sprintf( '%0'.DATA_LEN.'d',         $data_num++ ),
            sprintf( '%0'.VISITOR_ID_LEN.'d',   int( rand MAX_LISTS ) ),
            undef,
            gettimeofday + 0,
            );

        while ( scalar( @data ) * DATA_LEN > $coll->size )
        {
            while ( scalar( @data ) * DATA_LEN > $coll->size - $coll->advance_cleanup_bytes )
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

    is "@real_data", "@data", 'everything is working properly ('.( scalar @data ).' The remaining elements)';
}

test_insert( 0 );                               #-- only MAX_SIZE
test_insert( advance_cleanup_bytes );           #-- MAX_SIZE and advance_cleanup_bytes

#-- big_data_threshold

test_insert( 0, 11 );                           #-- only MAX_SIZE
test_insert( advance_cleanup_bytes, 11 );       #-- MAX_SIZE and advance_cleanup_bytes

}
