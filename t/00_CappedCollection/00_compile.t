#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 16;

BEGIN { use_ok 'Redis::CappedCollection' }

can_ok( 'Redis::CappedCollection', $_ ) foreach qw(
    new
    insert
    update
    receive
    validate
    pop_oldest
    exists
    lists
    drop
    quit

    max_datasize
    last_errorcode
    name
    size
    );

my $val;
ok( $val = Redis::CappedCollection::MAX_DATASIZE, "import OK: $val" );
