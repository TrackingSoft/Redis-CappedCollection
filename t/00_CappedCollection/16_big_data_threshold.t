#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

use Test::More;
plan "no_plan";

BEGIN {
    eval "use Test::Exception";                 ## no critic
    plan skip_all => "because Test::Exception required for testing" if $@;
}

BEGIN {
    eval "use Test::RedisServer";               ## no critic
    plan skip_all => "because Test::RedisServer required for testing" if $@;
}

BEGIN {
    eval "use Net::EmptyPort";                  ## no critic
    plan skip_all => "because Net::EmptyPort required for testing" if $@;
}

use bytes;
use Data::UUID;
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

use Redis::CappedCollection::Test::Utils qw(
    get_redis
    verify_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [] )

my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
isa_ok( $redis, 'Test::RedisServer' );

my ( $coll, $status_key, $queue_key, $time_key, $size, $big_data_threshold, $list_id, @arr );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

sub new_connect {
    # For Test::RedisServer
    $redis->stop if $redis;
    $redis = get_redis( conf =>
        {
            port                => Net::EmptyPort::empty_port( DEFAULT_PORT ),
            maxmemory           => 0,
            "maxmemory-policy"  => 'noeviction',
            "maxmemory-samples" => 100,
        } );
    isa_ok( $redis, 'Test::RedisServer' );

    $coll = Redis::CappedCollection->new(
        $redis,
        size                => $size,
        big_data_threshold  => $big_data_threshold,
        );
    isa_ok( $coll, 'Redis::CappedCollection' );

    ok ref( $coll->_redis ) =~ /Redis/, $msg;

    $status_key = NAMESPACE.':status:'.$coll->name;
    $queue_key  = NAMESPACE.':queue:'.$coll->name;
    $time_key   = NAMESPACE.':T:'.$coll->name.':'.$list_id;
    ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
    ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";
}

$big_data_threshold = 0;
$size = 0;
$list_id = '';
new_connect();
is $coll->big_data_threshold, 0, $msg;
$coll->drop_collection;

$big_data_threshold = 12345;
new_connect();
is $coll->big_data_threshold, 12345, $msg;
$coll->drop_collection;

$big_data_threshold = 3;
$list_id = 'Some_id';
new_connect();
is $coll->big_data_threshold, 3, $msg;

#-- insert without displacement
for ( 1..( $big_data_threshold * 2 ) )
{
    $coll->insert( '*****', $list_id );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    is $time_type, ( $coll->receive( $list_id ) <= $coll->big_data_threshold ) ? 'none' : 'zset', "time_type OK ($time_type)";
}

#-- pop_oldest
for ( 1..( $big_data_threshold * 2 ) )
{
    $coll->pop_oldest( $list_id );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    if ( $coll->receive( $list_id ) )
    {
        is $time_type, ( $coll->receive( $list_id ) <= $coll->big_data_threshold ) ? 'none' : 'zset', "time_type OK ($time_type)";
    }
    else
    {
        is $time_type, 'none', "time_type OK ($time_type)";
    }
}

$coll->drop_collection;

#-- insert with displacement
$big_data_threshold = 3;
$size = $big_data_threshold * 2;
new_connect();

for ( 1..( $size ) )
{
    $coll->insert( '*', $list_id );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    is $time_type, ( $coll->receive( $list_id ) <= $coll->big_data_threshold ) ? 'none' : 'zset', "time_type OK ($time_type)";
}

for ( 1..( $size ) )
{
    $coll->insert( '*' x $_, $list_id );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    is $time_type, ( $coll->receive( $list_id ) > $coll->big_data_threshold ) ? 'zset' : 'none', "time_type OK ($time_type)";
}

$coll->drop_collection;

#-- update with displacement
$big_data_threshold = 3;
$size = $big_data_threshold * 2;
new_connect();

for ( 1..( $size ) )
{
    $coll->insert( '*', $list_id );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    is $time_type, ( $coll->receive( $list_id ) <= $coll->big_data_threshold ) ? 'none' : 'zset', "time_type OK ($time_type)";
}

for ( 1..( $size ) )
{
    $coll->update( $list_id, $size - 1, '*' x $_ );
    my $time_type = $coll->_call_redis( "TYPE", $time_key );
    is $time_type, ( $coll->receive( $list_id ) > $coll->big_data_threshold ) ? 'zset' : 'none', "time_type OK ($time_type)";
}

$coll->drop_collection;

$big_data_threshold = 3;
$size = 0;
new_connect();

for ( 1..$big_data_threshold )
{
    $coll->insert( $_, $list_id );
}

for ( 1..$big_data_threshold )
{
    $coll->update( $list_id, $_ - 1, "*$_*" );
}

@arr = sort $coll->receive( $list_id );
is "@arr", "*1* *2* *3*", "update correct";

}
