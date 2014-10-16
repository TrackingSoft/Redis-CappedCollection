#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib', 't/tlib';

use Test::More;
plan "no_plan";

BEGIN {
    eval 'use Test::NoWarnings';                ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

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
use Time::HiRes     qw( gettimeofday );
use Params::Util    qw( _NONNEGINT );
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
    verify_redis
);

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", 9999999999999999, \"scalar", [], $uuid )

my ( $redis, $skip_msg, $port ) = verify_redis();

SKIP: {
    diag $skip_msg if $skip_msg;
    skip( $skip_msg, 1 ) if $skip_msg;

# For Test::RedisServer
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
ok $coll->_call_redis( "EXISTS", $status_key ), "status hash created";
ok !$coll->_call_redis( "EXISTS", $queue_key ), "queue list not created";

# all correct
$id = $coll->insert( "Some stuff", "Some id" );
is $id, "Some id", "correct result";
ok $coll->_call_redis( "EXISTS", $queue_key ), "queue list created";
foreach my $type ( qw( I D T ) )
{
    $list_key = NAMESPACE.":$type:".$coll->name.':'.$id;
    ok $coll->_call_redis( "EXISTS", $list_key ), "data list created";
}
is $coll->_call_redis( "HGET", $status_key, 'length' ), bytes::length( "Some stuff" ), "correct status value";
is $coll->_call_redis( "HGET", $status_key, 'lists'  ), 1, "correct status value";
ok defined( _NONNEGINT( $coll->_call_redis( "HGET", $status_key, 'size' ) ) ), "correct status value";

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

my $saved_name = $coll->name;
( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp, 0, "data id correct" );
( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp, 1, "data id correct" );
( undef, $tmp ) = $coll->insert( "Stuff", "ID" );
is( $tmp, 2, "data id correct" );

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

foreach my $arg ( ( "", "Some:id", \"scalar", [], $uuid ) )
{
    dies_ok { $coll->insert(
        "Correct stuff",
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

foreach my $arg ( ( \"scalar", [], $uuid ) )
{
    dies_ok { $coll->insert(
        "Correct stuff",
        "List id",
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

$tmp = 0;
foreach my $arg ( ( 0, -1, -3, "", "0", \"scalar", [], $uuid ) )
{
    dies_ok { $coll->insert(
        "Correct stuff",
        "List id",
        $tmp++,
        $arg,
        ) } "expecting to die: ".( $arg || '' );
}

# make sure that the data is saved and not destroyed
is $coll->_call_redis( "ZCOUNT", $queue_key, '-inf', '+inf' ), 4, "correct list number";
is $coll->_call_redis( "HGET", $status_key, 'lists' ), 4, "correct status value";

$list_key = NAMESPACE.":I:".$saved_name.':ID';
is $coll->_call_redis( "HGET", $list_key, 'n' ), 3, "correct info value";
is $coll->_call_redis( "HGET", $list_key, 'l' ), 0, "correct info value";
ok( $coll->_call_redis( "HGET", $list_key, 't' ) > 0, "correct info value" );
is $coll->_call_redis( "HGET", $list_key, 'i' ), 0, "correct info value";

$list_key = NAMESPACE.":D:".$saved_name.':ID';
is $coll->_call_redis( "HLEN", $list_key ), 3, "correct data number";

$list_key = NAMESPACE.":T:".$saved_name.':ID';
is $coll->_call_redis( "ZCOUNT", $list_key, '-inf', '+inf' ), 3, "correct time number";

foreach my $type ( qw( I D T ) )
{
    @arr = $coll->_call_redis( "KEYS", NAMESPACE.":$type:$saved_name:*" );
    is( scalar( @arr ), 4, "correct number of lists created" );
}

@arr = sort $coll->_call_redis( "ZRANGE", $queue_key, 0, -1 );
$tmp = "@arr";
@arr = sort ( "Some id", "Some new id", $id, "ID" );
is $tmp, "@arr", "correct values set";

$list_key = NAMESPACE.":D:".$saved_name.':Some id';
@arr = sort $coll->_call_redis( "HVALS", $list_key );
$tmp = "@arr";
@arr = ( "Some new stuff", "Some stuff" );
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

$list_key = NAMESPACE.':I:*';
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

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

#-------------------------------------------------------------------------------
$coll = Redis::CappedCollection->new(
    $redis,
    size    => 5,
    );
isa_ok( $coll, 'Redis::CappedCollection' );
ok $coll->_server =~ /.+:$port$/, $msg;
ok ref( $coll->_redis ) =~ /Redis/, $msg;
$status_key  = NAMESPACE.':status:'.$coll->name;

$list_key = NAMESPACE.':D:*';
foreach my $i ( 1..( $coll->size * 2 ) )
{
    $id = $coll->insert( $i, $i, undef, gettimeofday + 0 );
    $id = $coll->insert( $i, $i, undef, gettimeofday + 0 );
    $tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
    @arr = $coll->_call_redis( "KEYS", $list_key );
    ok $tmp <= $coll->size, "correct lists value: ".( $i * 2 )." inserts, $tmp length, size = ".$coll->size.", ".scalar( @arr )." lists";
}
$tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
@arr = $coll->_call_redis( "KEYS", $list_key );
is $tmp, $coll->size, "correct length value";
is scalar( @arr ), 2, "correct lists value";

$id = $coll->insert( '*' x $coll->size );
$tmp = $coll->_call_redis( "HGET", $status_key, 'length' );
@arr = $coll->_call_redis( "KEYS", $list_key );
is $tmp, $coll->size, "correct length value";
is scalar( @arr ), 1, "correct lists value";

dies_ok { $id = $coll->insert( '*' x ( $coll->size + 1 ) ) } "expecting to die";

$coll->_call_redis( "DEL", $_ ) foreach $coll->_call_redis( "KEYS", NAMESPACE.":*" );

}
