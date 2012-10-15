#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Test::TCP";
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

use bytes;
use Data::UUID;
use Time::HiRes     qw( gettimeofday );
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

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my $redis;
my $real_redis;
my $port = empty_port();

eval { $real_redis = Redis->new( server => DEFAULT_SERVER.":".DEFAULT_PORT ) };
my $skip_msg;
$skip_msg = "Redis server is unavailable" unless ( !$@ and $real_redis and $real_redis->ping );
$skip_msg = "Need a Redis server version 2.6 or higher" if ( !$skip_msg and !eval { return $real_redis->eval( 'return 1', 0 ) } );

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For real Redis:
#$redis = $real_redis;
#isa_ok( $redis, 'Redis' );

# For Test::RedisServer
$real_redis->quit;
$redis = Test::RedisServer->new( conf => { port => $port }, timeout => 3 );
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr, $len, $info, $tm );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

for my $big_data_threshold ( ( 0, 10 ) )
{

    $coll = Redis::CappedCollection->new(
        $redis,
        big_data_threshold => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );
    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key  = NAMESPACE.':status:'.$coll->name;
    $queue_key   = NAMESPACE.':queue:'.$coll->name;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

#-- all correct

    $info = $coll->info( 'Some list id' );
    is $info->{items},              undef, "OK items";
    is $info->{oldest_time},        undef, "OK oldest_time";
    is $info->{last_removed_time},  undef, "OK last_removed_time";

# some inserts
    $len = 0;
    $tmp = 0;
    $tm = gettimeofday;
    $coll->insert( 1, 'Some list id', '', $tm );
    $info = $coll->info( 'Some list id' );
    is $info->{items}, 1, "OK items";
    ok abs( $tm - $info->{oldest_time} <= 0.00009 ), "OK oldest_time";
    is $info->{last_removed_time},  0,      "OK last_removed_time";
    for ( my $i = 2; $i <= 10; ++$i )
    {
        $coll->insert( $i, 'Some list id', undef, gettimeofday + 0 );
        $info = $coll->info( 'Some list id' );
        is $info->{items}, $i, "OK items";
        ok abs( $tm - $info->{oldest_time} <= 0.00009 ), "OK oldest_time";
        is $info->{last_removed_time}, 0, "OK last_removed_time";
    }

    $coll->drop_collection;

# Remove old data (insert)
    $coll = Redis::CappedCollection->new(
        $redis,
        size    => 5,
        big_data_threshold => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );
    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    @arr = ();
    foreach my $i ( 1..$coll->size )
    {
        $tm = gettimeofday;
        push @arr, $tm;
        $id = $coll->insert( '*', 'Some list id', '', $tm );
        $info = $coll->collection_info;
    }
    $id = $coll->insert( '*', 'Some list id', '', $tm );

    $info = $coll->info( 'Some list id' );
    is $info->{items}, $coll->size, "OK items";
    ok abs( $arr[1] - $info->{oldest_time} ) <= 0.00009, "OK oldest_time";
    ok abs( $arr[0] - $info->{last_removed_time} ) <= 0.00009, "OK oldest_time";

    dies_ok { $coll->info() } "expecting to die - no args";

    foreach my $arg ( ( undef, "", \"scalar", [], $uuid ) )
    {
        dies_ok { $coll->info(
            $arg,
            ) } "expecting to die: ".( $arg || '' );
    }

    $coll->drop_collection;

#----------
    $coll = Redis::CappedCollection->new(
        $redis,
        big_data_threshold => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );
    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

# some inserts
    $len = 0;
    $tmp = 0;
    $tm = time;
    for ( my $i = 1; $i <= 10; ++$i )
    {
        $coll->insert( $i, 'Some list id', undef, $tm );
        $info = $coll->info( 'Some list id' );
        is $info->{items}, $i, "OK items";
        is $info->{oldest_time}, $tm, "OK oldest_time";
        is $info->{last_removed_time}, 0, "OK last_removed_time";
    }

    $coll->drop_collection;

# Remove old data (insert)
    $coll = Redis::CappedCollection->new(
        $redis,
        size    => 5,
        big_data_threshold => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );
    ok $coll->_server =~ /.+:$port$/, $msg;
    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    @arr = ();
    $tm = time;
    foreach my $i ( 1..$coll->size )
    {
        push @arr, $tm;
        $id = $coll->insert( '*', 'Some list id', undef, $tm );
    }
    $id = $coll->insert( '*', 'Some list id', undef, $tm );

    $info = $coll->info( 'Some list id' );
    is $info->{items}, $coll->size, "OK items";
    is $info->{oldest_time}, $tm, "OK oldest_time";
    is $info->{last_removed_time}, $tm, "OK oldest_time";

    $coll->drop_collection;

}

}
