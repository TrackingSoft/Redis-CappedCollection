#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

use lib 'lib';

use Test::More tests => 20;

BEGIN {
    eval 'use Test::NoWarnings';    ## no critic
    plan skip_all => 'because Test::NoWarnings required for testing' if $@;
}

BEGIN { use_ok 'Redis::CappedCollection', qw(
    $DATA_VERSION
    $DEFAULT_SERVER
    $DEFAULT_PORT
    $NAMESPACE
    $MIN_MEMORY_RESERVE
    $MAX_MEMORY_RESERVE

    $ENOERROR
    $EINCOMPDATAVERSION
    $EMISMATCHARG
    $EDATATOOLARGE
    $ENETWORK
    $EMAXMEMORYLIMIT
    $EMAXMEMORYPOLICY
    $ECOLLDELETED
    $EREDIS
    $EDATAIDEXISTS
    $EOLDERTHANALLOWED
    $ENONEXISTENTDATAID
    ) }

my $val;

ok( defined( $_ ), "import OK: $_" ) foreach qw(
    $DATA_VERSION
    $DEFAULT_SERVER
    $DEFAULT_PORT
    $NAMESPACE
    $MIN_MEMORY_RESERVE
    $MAX_MEMORY_RESERVE

    $ENOERROR
    $EINCOMPDATAVERSION
    $EMISMATCHARG
    $EDATATOOLARGE
    $ENETWORK
    $EMAXMEMORYLIMIT
    $EMAXMEMORYPOLICY
    $ECOLLDELETED
    $EREDIS
    $EDATAIDEXISTS
    $EOLDERTHANALLOWED
    $ENONEXISTENTDATAID
    );
