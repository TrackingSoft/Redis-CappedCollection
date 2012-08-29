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

my ( $coll, $name, $tmp, $id, $status_key, $queue_key, $list_key, @arr );
my $uuid = new Data::UUID;
my $msg = "attribute is set correctly";

$coll = Redis::CappedCollection->new(
    $redis,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

$status_key  = NAMESPACE.':status:'.$coll->name;
$queue_key   = NAMESPACE.':queue:'.$coll->name;
my $saved_name = $coll->name;
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

# all correct
$id = $coll->insert( "Some stuff", "Some id" );
is $id, "Some id", "correct result";
$list_key = NAMESPACE.':L:'.$coll->name.':'.$id;
ok $coll->_call_redis( "EXISTS", $queue_key ), "queue list created";
ok $coll->_call_redis( "EXISTS", $list_key ), "data list created";
is $coll->_call_redis( "HGET", $status_key, 'length' ), bytes::length( "Some stuff" ), "correct status value";
is $coll->_call_redis( "HGET", $status_key, 'lists'  ), 1, "correct status value";

$tmp = $coll->insert( "Some new stuff", "Some id" );
is $tmp, $id, "correct result";
is $coll->_call_redis( "HGET", $status_key, 'length' ), bytes::length( "Some stuff"."Some new stuff" ), "correct status value";
is $coll->_call_redis( "HGET", $status_key, 'lists'  ), 1, "correct status value";

$tmp = $coll->insert( "Some another stuff", "Some new id" );
is $tmp, "Some new id", "correct result";
is $coll->_call_redis( "HGET", $status_key, 'length' ), bytes::length( "Some stuff"."Some new stuff"."Some another stuff" ), "correct status value";
is $coll->_call_redis( "HGET", $status_key, 'lists'  ), 2, "correct status value";

$id = $coll->insert( "Any stuff" );
is bytes::length( $id ), bytes::length( '89116152-C5BD-11E1-931B-0A690A986783' ), $msg;

( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp + 1, 1, "list len correct" );
( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp + 1, 2, "list len correct" );
( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp + 1, 3, "list len correct" );

# errors in the arguments
$coll = Redis::CappedCollection->new(
    $redis,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;

dies_ok { $coll->insert() } "expecting to die - no args";

foreach my $arg ( ( undef, \"scalar", [], $uuid ) )
{
    dies_ok { $coll->insert(
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

foreach my $arg ( ( \"scalar", [], $uuid ) )
{
    dies_ok { $coll->insert(
        "Correct stuff",
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

# make sure that the data is saved and not destroyed
is $coll->_call_redis( "LLEN", $queue_key  ), 7, "correct status value";
is $coll->_call_redis( "LLEN", $list_key   ), 2, "correct status value";
$tmp = NAMESPACE.':L:'.$coll->name;
@arr = $coll->_call_redis( "KEYS", NAMESPACE.':L:'.$saved_name.':*' );
is( scalar( @arr ), 4, "correct number of lists created" );

@arr = $coll->_call_redis( "LRANGE", $queue_key, 0, -1 );
$tmp = "@arr";
@arr = ( "Some id", "Some id", "Some new id", $id, "ID", "ID", "ID" );
is $tmp, "@arr", "correct values set";

@arr = $coll->_call_redis( "LRANGE", $list_key, 0, -1 );
$tmp = "@arr";
@arr = ( "Some stuff", "Some new stuff" );
is $tmp, "@arr", "correct values set";

# destruction of status hash
$status_key  = NAMESPACE.':status:'.$coll->name;
$coll->_call_redis( "DEL", $status_key );
dies_ok { $id = $coll->insert( "Some stuff", "Some id" ) } "expecting to die";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

# Remove old data
$coll = Redis::CappedCollection->new(
    $redis,
    size    => 5,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;
$status_key  = NAMESPACE.':status:'.$coll->name;

$list_key = NAMESPACE.':L:*';
foreach my $i ( 1..( $coll->size * 2 ) )
{
    $id = $coll->insert( $i, $i );
    $tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
    @arr = $coll->_call_redis( "KEYS", $list_key );
    ok $tmp <= $coll->size, "correct lists value: $i inserts, $tmp length, size = ".$coll->size.", ".scalar( @arr )." lists";
}
$tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
@arr = $coll->_call_redis( "KEYS", $list_key );
is $tmp, $coll->size, "correct length value";
is scalar( @arr ), 4, "correct lists value";

$id = $coll->insert( '*' x $coll->size );
$tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
@arr = $coll->_call_redis( "KEYS", $list_key );
is $tmp, $coll->size, "correct length value";
is scalar( @arr ), 1, "correct lists value";

dies_ok { $id = $coll->insert( '*' x ( $coll->size + 1 ) ) } "expecting to die";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
