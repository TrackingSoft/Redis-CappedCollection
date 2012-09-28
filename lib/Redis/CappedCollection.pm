package Redis::CappedCollection;
use 5.010;

# Pragmas
use strict;
use warnings;
use bytes;

our $VERSION = '0.01';

use Exporter qw( import );
our @EXPORT_OK  = qw(
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

#-- load the modules -----------------------------------------------------------

# Modules
use Mouse;
use Mouse::Util::TypeConstraints;
use Carp;
use Redis;
use Data::UUID;
use Time::HiRes     qw( gettimeofday );
use Digest::SHA1    qw( sha1_hex );
use List::Util      qw( min );
use Params::Util    qw( _NONNEGINT _STRING _INSTANCE _NUMBER );

#-- declarations ---------------------------------------------------------------

use constant {
    DEFAULT_SERVER      => 'localhost',
    DEFAULT_PORT        => 6379,

    NAMESPACE           => 'Capped',
    MAX_DATASIZE        => 512*1024*1024,       # A String value can be at max 512 Megabytes in length.

    ENOERROR            => 0,
    EMISMATCHARG        => 1,
    EDATATOOLARGE       => 2,
    ENETWORK            => 3,
    EMAXMEMORYLIMIT     => 4,
    EMAXMEMORYPOLICY    => 5,
    ECOLLDELETED        => 6,
    EREDIS              => 7,
    EDATAIDEXISTS       => 8,
    EOLDERTHANALLOWED   => 9,
    };

my @ERROR = (
    'No error',
    'Mismatch argument',
    'Data is too large',
    'Error in connection to Redis server',
    "Command not allowed when used memory > 'maxmemory'",
    'Data may be removed by maxmemory-policy all*',
    'Collection was removed prior to use',
    'Redis error message',
    'Attempt to add data to an existing ID',
    'Attempt to add data over outdated',
    );

my $uuid = new Data::UUID;

my %lua_script_body;
my $_lua_cleaning = <<"END_CLEANING";
        if redis.call( 'HGET', status_key, 'length' ) + size_delta > size then
            local size_limit = size - size_garbage
            while redis.call( 'EXISTS', status_key ) and redis.call( 'HGET', status_key, 'length' ) + size_delta > size_limit do
                local vals              = redis.call( 'ZRANGE', queue_key, 0, 0 )
                local excess_list_id    = vals[1]
                local excess_info_key   = info_keys..':'..excess_list_id
                local excess_data_key   = data_keys..':'..excess_list_id
                local excess_time_key   = time_keys..':'..excess_list_id
                local excess_data_id    = redis.call( 'HGET', excess_info_key, 'oldest_data_id' )
                local excess_data       = redis.call( 'HGET', excess_data_key, excess_data_id )
                local excess_data_len   = #excess_data
                local excess_data_time  = redis.call( 'ZSCORE', excess_time_key, excess_data_id )
                redis.call( 'HDEL',     excess_data_key, excess_data_id )
                redis.call( 'HSET',     excess_info_key, 'last_removed_time', excess_data_time )
                redis.call( 'HINCRBY',  status_key, 'items', -1 )
                if redis.call( 'EXISTS', excess_data_key ) == 1 then
                    redis.call( 'ZREM',     excess_time_key, excess_data_id )
                    vals                    = redis.call( 'ZRANGE', excess_time_key, 0, 0, 'WITHSCORES' )
                    local oldest_data_id    = vals[1]
                    local oldest_time       = vals[2]
                    redis.call( 'HMSET',    excess_info_key, 'oldest_time', oldest_time, 'oldest_data_id', oldest_data_id )
                    redis.call( 'ZADD',     queue_key, oldest_time, excess_list_id )
                else
                    redis.call( 'DEL',      excess_info_key, excess_time_key )
                    redis.call( 'HINCRBY',  status_key, 'lists', -1 )
                    redis.call( 'ZREM',     queue_key, excess_list_id )
                end
                redis.call( 'HINCRBY', status_key, 'length', -excess_data_len )
            end
            list_exist = redis.call( 'EXISTS', data_key )
        end
END_CLEANING

my $_lua_namespace  = "local namespace  = '".NAMESPACE."'";
my $_lua_queue_key  = "local queue_key  = namespace..':queue:'..coll_name";
my $_lua_status_key = "local status_key = namespace..':status:'..coll_name";
my $_lua_info_keys  = "local info_keys  = namespace..':I:'..coll_name";
my $_lua_data_keys  = "local data_keys  = namespace..':D:'..coll_name";
my $_lua_time_keys  = "local time_keys  = namespace..':T:'..coll_name";
my $_lua_info_key   = "local info_key   = info_keys..':'..list_id";
my $_lua_data_key   = "local data_key   = data_keys..':'..list_id";
my $_lua_time_key   = "local time_key   = time_keys..':'..list_id";

$lua_script_body{insert} = <<"END_INSERT";
local coll_name         = ARGV[1]
local size_garbage      = ARGV[2] + 0
local list_id           = ARGV[3]
local data_id           = ARGV[4]
local data              = ARGV[5]
local data_time         = ARGV[6] + 0
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
$_lua_info_key
$_lua_data_key
$_lua_time_key
local status_exist      = redis.call( 'EXISTS', status_key )
local list_exist        = redis.call( 'EXISTS', data_key )
local error             = 0
local result_data_id    = 0
if status_exist == 1 then
    local data_len      = #data
    local size_delta    = data_len
    local size          = redis.call( 'HGET', status_key, 'size' ) + 0
    if size > 0 then
        $_lua_cleaning
    end
    if data_id == '' then
        if list_exist ~= 0 then
            result_data_id = redis.call( 'HGET', info_key, 'next_data_id' )
        end
    else
        result_data_id = data_id
    end
    if redis.call( 'HEXISTS', data_key, result_data_id ) == 1 then
        error = 8               -- EDATAIDEXISTS
    end
    if error == 0 then
        if redis.call( 'HGET', status_key, 'older_allowed' ) ~= '1' then
            if list_exist ~= 0 then
                local last_removed_time = redis.call( 'HGET', info_key, 'last_removed_time' ) + 0
                if last_removed_time > 0 and data_time <= last_removed_time then
                    error = 9   -- EOLDERTHANALLOWED
                end
            end
        end
        if error == 0 then
            if list_exist == 0 then
                redis.call( 'HMSET',    info_key, 'next_data_id', 0, 'last_removed_time', 0, 'oldest_time', 0, 'oldest_data_id', '' )
                redis.call( 'HINCRBY',  status_key, 'lists', 1 )
            end
                if data_id == '' then
                    redis.call( 'HINCRBY', info_key, 'next_data_id', 1 )
                end
            redis.call( 'HINCRBY', status_key, 'length', data_len )
            if redis.call( 'HEXISTS', status_key, 'size' ) == 0 then
                redis.call( 'HMSET', status_key, 'size', size, 'items', 0 )
            end
            redis.call( 'HSET',     data_key, result_data_id, data )
            redis.call( 'HINCRBY',  status_key, 'items', 1 )
            redis.call( 'ZADD',     time_key, data_time, result_data_id )
            local oldest_time       = redis.call( 'HGET', info_key, 'oldest_time' ) + 0
            if data_time < oldest_time or oldest_time == 0 then
                redis.call( 'HMSET', info_key, 'oldest_time', data_time, 'oldest_data_id', result_data_id )
                if redis.call( 'HEXISTS', info_key, 'next_data_id' ) == 0 then
                        redis.call( 'HSET', info_key, 'next_data_id', 0 )
                end
                redis.call( 'ZADD', queue_key, data_time, list_id )
            end
        end
    end
end
return { error, status_exist, result_data_id }
END_INSERT

$lua_script_body{update} = <<"END_UPDATE";
local coll_name     = ARGV[1]
local size_garbage  = ARGV[2] + 0
local list_id       = ARGV[3]
local data_id       = ARGV[4]
local data          = ARGV[5]
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
$_lua_info_key
$_lua_data_key
$_lua_time_key
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', data_key )
local error         = 0
local ret           = 0
if status_exist == 1 then
    if redis.call( 'HEXISTS', data_key, data_id ) == 1 then
        local data_len      = #data
        local cur_data      = redis.call( 'HGET', data_key, data_id )
        local cur_data_len  = #cur_data
        local size_delta    = data_len - cur_data_len
        local size          = redis.call( 'HGET', status_key, 'size' ) + 0
        if size > 0 then
            $_lua_cleaning
        end
        if list_exist ~= 0 and redis.call( 'HEXISTS', data_key, data_id ) == 1 then
            ret = redis.call( 'HSET', data_key, data_id, data )
            if ret == 0 then
                redis.call( 'HINCRBY', status_key, 'length', size_delta )
                ret = 1
            else
                ret = 0
            end
        end
    end
end
return { status_exist, ret }
END_UPDATE

$lua_script_body{pop_oldest} = <<"END_POP_OLDEST";
local coll_name         = ARGV[1]
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
local status_exist      = redis.call( 'EXISTS', status_key )
local queue_exist       = redis.call( 'EXISTS', queue_key )
local excess_list_exist = 0
local excess_list_id    = false
local excess_data       = false
if queue_exist == 1 and status_exist == 1 then
    excess_list_id          = redis.call( 'ZRANGE', queue_key, 0, 0 )[1]
    local excess_info_key   = info_keys..':'..excess_list_id
    local excess_data_key   = data_keys..':'..excess_list_id
    local excess_time_key   = time_keys..':'..excess_list_id
    excess_list_exist       = redis.call( 'EXISTS', excess_data_key )
    local excess_data_id    = redis.call( 'HGET', excess_info_key, 'oldest_data_id' )
    excess_data             = redis.call( 'HGET', excess_data_key, excess_data_id )
    local excess_data_len   = #excess_data
    local excess_data_time  = redis.call( 'ZSCORE', excess_time_key, excess_data_id )
    redis.call( 'HDEL',     excess_data_key, excess_data_id )
    redis.call( 'HSET', excess_info_key, 'last_removed_time', excess_data_time )
    redis.call( 'HINCRBY',  status_key, 'items', -1 )
    if redis.call( 'EXISTS', excess_data_key ) == 1 then
        redis.call( 'ZREM', excess_time_key, excess_data_id )
        local oldest_data_id
        local oldest_time
        local vals      = redis.call( 'ZRANGE', excess_time_key, 0, 0, 'WITHSCORES' )
        oldest_data_id  = vals[1]
        oldest_time     = vals[2]
        redis.call( 'HMSET',    excess_info_key, 'oldest_time', oldest_time, 'oldest_data_id', oldest_data_id )
        redis.call( 'ZADD',     queue_key, oldest_time, excess_list_id )
    else
        redis.call( 'DEL',      excess_info_key, excess_time_key )
        redis.call( 'HINCRBY',  status_key, 'lists', -1 )
        redis.call( 'ZREM',     queue_key, excess_list_id )
    end
    redis.call( 'HINCRBY', status_key, 'length', -excess_data_len )
end
return { queue_exist, status_exist, excess_list_exist, excess_list_id, excess_data }
END_POP_OLDEST

$lua_script_body{validate} = <<"END_VALIDATE";
local coll_name     = ARGV[1]
$_lua_namespace
$_lua_queue_key
$_lua_status_key
local status_exist  = redis.call( 'EXISTS', status_key )
if status_exist == 1 then
    local length = redis.call( 'HGET', status_key, 'length' )
    local lists  = redis.call( 'HGET', status_key, 'lists' )
    local items  = redis.call( 'HGET', status_key, 'items' )
    return { status_exist, length, lists, items }
end
return { status_exist, false, false, false }
END_VALIDATE

$lua_script_body{drop} = <<"END_DROP";
local coll_name         = ARGV[1]
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
local ret           = 0
local arr
if redis.call( 'EXISTS', status_key ) == 1 then
    ret = ret + redis.call( 'DEL', queue_key, status_key )
end
arr = redis.call( 'KEYS', info_keys..':*' )
if #arr > 0 then
    for i = 1, #arr do
        ret = ret + redis.call( 'DEL', arr[i] )
    end
    local patterns = { data_keys, time_keys }
    local key
    local i
    for k = 1, #patterns do
        arr = redis.call( 'KEYS', patterns[k]..':*' )
        for i = 1, #arr do
            ret = ret + redis.call( 'DEL', arr[i] )
        end
    end
end
return ret
END_DROP

$lua_script_body{verify_collection} = <<"END_VERIFY_COLLECTION";
local coll_name     = ARGV[1]
local size          = ARGV[2]
local older_allowed = ARGV[3]
$_lua_namespace
$_lua_status_key
local status_exist  = redis.call( 'EXISTS', status_key )
if status_exist == 1 then
    size            = redis.call( 'HGET', status_key, 'size' )
    older_allowed   = redis.call( 'HGET', status_key, 'older_allowed' )
else
    redis.call( 'HMSET', status_key, 'size', size, 'length', 0, 'lists', 0, 'items', 0, 'older_allowed', older_allowed )
end
return { status_exist, size, older_allowed }
END_VERIFY_COLLECTION

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
    ;

subtype __PACKAGE__.'::NonEmptNameStr',
    as 'Str',
    where { $_ ne '' and $_ !~ /:/ },
    message { ( $_ || '' ).' is not a non-empty string!' }
    ;

subtype __PACKAGE__.'::DataStr',
    as 'Str',
    where { bytes::length( $_ ) <= MAX_DATASIZE },
    message { "'".( $_ || '' )."' is not a valid data string!" }
    ;

#-- constructor ----------------------------------------------------------------

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;

    if ( _INSTANCE( $_[0], 'Redis' ) )
    {
        my $redis = shift;
        return $class->$orig(
# have to look into the Redis object ...
            redis   => $redis->{server},
            _redis  => $redis,
            @_
            );
    }
    elsif ( _INSTANCE( $_[0], 'Test::RedisServer' ) )
    {
# to test only
        my $redis = shift;
        return $class->$orig(
# have to look into the Test::RedisServer object ...
            redis   => '127.0.0.1:'.$redis->conf->{port},
            @_
            );
    }
    elsif ( _INSTANCE( $_[0], __PACKAGE__ ) )
    {
        my $coll = shift;
        return $class->$orig(
                    redis   => $coll->_server,
                    _redis  => $coll->_redis,
                    @_
                );
    }
    else
    {
        return $class->$orig( @_ );
    }
};

sub BUILD {
    my $self = shift;

    $self->_redis( $self->_redis_constructor )
        unless ( $self->_redis );

    my ( $major, $minor ) = $self->_redis->info->{redis_version} =~ /^(\d+)\.(\d+)/;
    if ( $major < 2 or ( $major == 2 and $minor <= 4 ) )
    {
        $self->_set_last_errorcode( EREDIS );
        confess "Need a Redis server version 2.6 or higher";
    }

    $self->_maxmemory_policy( ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory-policy' ) )[1] );
    if ( $self->_maxmemory_policy =~ /^all/ )
    {
        $self->_throw( EMAXMEMORYPOLICY );
    }
    $self->_maxmemory(        ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory'        ) )[1] );
    $self->max_datasize( min $self->_maxmemory, $self->max_datasize )
        if $self->_maxmemory;

    $self->_queue_key(  NAMESPACE.':queue:'.$self->name );
    $self->_status_key( NAMESPACE.':status:'.$self->name );
    $self->_info_keys(  NAMESPACE.':I:'.$self->name );
    $self->_data_keys(  NAMESPACE.':D:'.$self->name );
    $self->_time_keys(  NAMESPACE.':T:'.$self->name );

    $self->_verify_collection;
}

#-- public attributes ----------------------------------------------------------

has 'name' => (
    is          => 'ro',
    clearer     => '_clear_name',
    isa         => __PACKAGE__.'::NonEmptNameStr',
    default     => sub { return $uuid->create_str },
    );

has 'size'              => (
    reader      => 'size',
    writer      => '_set_size',
    clearer     => '_clear_size',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    );

has 'size_garbage'              => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    trigger     => sub {
                        my $self = shift;
                        $self->size_garbage <= $self->size || $self->_throw( EMISMATCHARG, 'size_garbage' );
                    },
    );

has 'max_datasize'      => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => MAX_DATASIZE,
    trigger     => sub {
                        my $self = shift;
                        if ( $self->_redis )
                        {
                            $self->max_datasize <= ( $self->_maxmemory ? min( $self->_maxmemory, MAX_DATASIZE ) : MAX_DATASIZE )
                                || $self->_throw( EMISMATCHARG, 'max_datasize' );
                        }
                    },
    );

has 'older_allowed' => (
    is          => 'ro',
    isa         => 'Bool',
    default     => 1,
    );

has 'last_errorcode'    => (
    reader      => 'last_errorcode',
    writer      => '_set_last_errorcode',
    isa         => 'Int',
    default     => 0,
    );

#-- private attributes ---------------------------------------------------------

has '_server'           => (
    is          => 'rw',
    init_arg    => 'redis',
    isa         => 'Str',
    default     => DEFAULT_SERVER.':'.DEFAULT_PORT,
    trigger     => sub {
                        my $self = shift;
                        $self->_server( $self->_server.':'.DEFAULT_PORT )
                            unless $self->_server =~ /:/;
                    },
    );

has '_redis'            => (
    is          => 'rw',
# 'Maybe[Test::RedisServer]' to test only
    isa         => 'Maybe[Redis] | Maybe[Test::RedisServer]',
    init_arg    => undef,
    );

has '_maxmemory' => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    init_arg    => undef,
    );

foreach my $attr_name ( qw(
    _maxmemory_policy
    _queue_key
    _status_key
    _info_keys
    _data_keys
    _time_keys
    ) )
{
    has $attr_name => (
        is          => 'rw',
        isa         => 'Str',
        init_arg    => undef,
        );
}

has '_lua_scripts' => (
    is          => 'ro',
    isa         => 'HashRef[Str]',
    lazy        => 1,
    init_arg    => undef,
    builder     => sub { return {}; },
    );

#-- public methods -------------------------------------------------------------

sub insert {
    my $self        = shift;
    my $data        = shift;
    my $list_id     = shift // $uuid->create_str;
    my $data_id     = shift // '';
    my $data_time   = shift // scalar gettimeofday;

    $data                                                   // $self->_throw( EMISMATCHARG, 'data' );
    ( defined( _STRING( $data ) ) or $data eq '' )          || $self->_throw( EMISMATCHARG, 'data' );
    _STRING( $list_id )                                     // $self->_throw( EMISMATCHARG, 'list_id' );
    $list_id !~ /:/                                         || $self->_throw( EMISMATCHARG, 'list_id' );
    ( defined( _STRING( $data_id ) ) or $data_id eq '' )    || $self->_throw( EMISMATCHARG, 'data_id' );
    ( defined( _NUMBER( $data_time ) ) and $data_time > 0 ) || $self->_throw( EMISMATCHARG, 'data_time' );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )          || $self->_throw( EMISMATCHARG, 'data' ) );
    ( $data_len <= $self->max_datasize )                    || $self->_throw( EDATATOOLARGE );

    $self->_set_last_errorcode( ENOERROR );

    my ( $error, $status_exist, $result_data_id ) = $self->_call_redis(
        $self->_lua_script_cmd( 'insert' ),
        0,
        $self->name,
        $self->size_garbage,
        $list_id,
        $data_id,
        $data,
        $data_time,
        );

    if ( $error ) { $self->_throw( $error ); }

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return wantarray ? ( $list_id, $result_data_id ) : $list_id;
}

sub update {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;
    my $data        = shift;

    _STRING( $list_id )                             // $self->_throw( EMISMATCHARG, 'list_id' );
    defined( _STRING( $data_id ) )                  || $self->_throw( EMISMATCHARG, 'data_id' );
    $data                                           // $self->_throw( EMISMATCHARG, 'data' );
    ( defined( _STRING( $data ) ) or $data eq '' )  || $self->_throw( EMISMATCHARG, 'data' );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )  || $self->_throw( EMISMATCHARG, 'data' ) );
    ( $data_len <= $self->max_datasize )            || $self->_throw( EDATATOOLARGE );

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $ret ) = $self->_call_redis(
        $self->_lua_script_cmd( 'update' ),
        0,
        $self->name,
        $self->size_garbage,
        $list_id,
        $data_id,
        $data,
        );

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return $ret;
}

sub receive {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    my $data_key = $self->_data_list_key( $list_id );
    if ( defined( $data_id ) and $data_id ne '' )
    {
        _STRING( $data_id ) // $self->_throw( EMISMATCHARG, 'data_id' );
        return $self->_call_redis( 'HGET', $data_key, $data_id );
    }
    else
    {
        if ( wantarray )
        {
            return $self->_call_redis( defined( $data_id ) ? 'HGETALL' : 'HVALS', $data_key );
        }
        else
        {
            return $self->_call_redis( 'HLEN', $data_key );
        }
    }
}

sub pop_oldest {
    my $self        = shift;

    my @ret;
    $self->_set_last_errorcode( ENOERROR );

    my ( $queue_exist, $status_exist, $excess_list_exist, $excess_id, $excess_data ) =
        $self->_call_redis(
            $self->_lua_script_cmd( 'pop_oldest' ),
            0,
            $self->name,
            );

    if ( $queue_exist )
    {
        unless ( $status_exist )
        {
            $self->_clear_sha1;
            $self->_throw( ECOLLDELETED );
        }
        unless ( $excess_list_exist )
        {
            $self->_clear_sha1;
            $self->_throw( EMAXMEMORYPOLICY );
        }
        @ret = ( $excess_id, $excess_data );
    }

    return @ret;
}

sub validate {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'validate' ),
        0,
        $self->name,
        );

    unless ( shift @ret )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return @ret;
}

sub exists {
    my $self        = shift;
    my $list_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    return $self->_call_redis( 'EXISTS', $self->_data_list_key( $list_id ) );
}

sub lists {
    my $self        = shift;
    my $pattern     = shift // '*';

    _STRING( $pattern ) // $self->_throw( EMISMATCHARG, 'pattern' );

    $self->_set_last_errorcode( ENOERROR );

    return map { ( $_ =~ /:([^:]+)$/ )[0] } $self->_call_redis( 'KEYS', $self->_data_list_key( $pattern ) );
}

sub drop {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    my $ret = $self->_call_redis(
        $self->_lua_script_cmd( 'drop' ),
        0,
        $self->name,
        );

    $self->_clear_name;
    $self->_clear_size;
    $self->_clear_sha1;

    return $ret;
}

sub quit {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );
    $self->_clear_sha1;
    eval { $self->_redis->quit };
    $self->_redis_exception( $@ )
        if $@;
}

#-- private methods ------------------------------------------------------------

sub _lua_script_cmd {
    my $self        = shift;
    my $name        = shift;

    my $sha1 = $self->_lua_scripts->{ $name };
    unless ( $sha1 )
    {
        $sha1 = $self->_lua_scripts->{ $name } = sha1_hex( $lua_script_body{ $name } );
        unless ( ( $self->_call_redis( 'SCRIPT', 'EXISTS', $sha1 ) )[0] )
        {
            return( 'EVAL', $lua_script_body{ $name } );
        }
    }
    return( 'EVALSHA', $sha1 );
}

sub _data_list_key {
    my $self        = shift;
    my $list_id     = shift;

    return( $self->_data_keys.':'.$list_id );
}

sub _verify_collection {
    my $self    = shift;

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $size, $older_allowed ) = $self->_call_redis(
        $self->_lua_script_cmd( 'verify_collection' ),
        0,
        $self->name,
        $self->size || 0,
        $self->older_allowed ? 1 : 0,
        );

    if ( $status_exist )
    {
        $self->_set_size( $size ) unless $self->size;
        $size           == $self->size          or $self->_throw( EMISMATCHARG, 'size' );
        $older_allowed  == $self->older_allowed or $self->_throw( EMISMATCHARG, 'older_allowed' );
    }
}

sub _throw {
    my $self    = shift;
    my $err     = shift;
    my $prefix  = shift;

    $self->_set_last_errorcode( $err );
    confess ( $prefix ? "$prefix : " : '' ).$ERROR[ $err ];
}

sub _redis_exception {
    my $self    = shift;
    my $error   = shift;

# Use the error messages from Redis.pm
    if (
           $error =~ /^Could not connect to Redis server at /
        or $error =~ /^Can't close socket: /
        or $error =~ /^Not connected to any server/
# Maybe for pub/sub only
        or $error =~ /^Error while reading from Redis server: /
        or $error =~ /^Redis server closed connection/
        )
    {
        $self->_set_last_errorcode( ENETWORK );
    }
    elsif (
           $error =~ /[\S+\s?]ERR command not allowed when used memory > 'maxmemory'/
        or $error =~ /[\S+\s?]OOM command not allowed when used memory > 'maxmemory'/
        or $error =~ /\[EVALSHA\] NOSCRIPT No matching script. Please use EVAL./
        )
    {
        $self->_set_last_errorcode( EMAXMEMORYLIMIT );
        $self->_clear_sha1;
    }
    else
    {
        $self->_set_last_errorcode( EREDIS );
    }

    die $error;
}

sub _clear_sha1 {
    my $self    = shift;

    delete @{$self->_lua_scripts}{ keys %{$self->_lua_scripts} };
}

sub _redis_constructor {
    my $self    = shift;

    $self->_set_last_errorcode( ENOERROR );
    my $redis;
    eval { $redis = Redis->new( server => $self->_server ) };
    $self->_redis_exception( $@ )
        if $@;
    return $redis;
}

# Keep in mind the default 'redis.conf' values:
# Close the connection after a client is idle for N seconds (0 to disable)
#    timeout 300

# Send a request to Redis
sub _call_redis {
    my $self        = shift;
    my $method      = shift;

    my @return;
    $self->_set_last_errorcode( ENOERROR );
    @return = eval { return $self->_redis->$method( map { ref( $_ ) ? $$_ : $_ } @_ ) };
    $self->_redis_exception( $@ )
        if $@;

    return wantarray ? @return : $return[0];
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

__END__

=head1 NAME

Redis::CappedCollection - Provides fixed sized collections that have
a auto-FIFO age-out feature.

=head1 VERSION

This documentation refers to C<Redis::CappedCollection> version 0.01

=head1 SYNOPSIS

    #-- Common
    use Redis::CappedCollection qw( DEFAULT_SERVER DEFAULT_PORT )

    my $server = DEFAULT_SERVER.':'.DEFAULT_PORT;
    my $coll = Redis::CappedCollection->new( redis => $server );

    #-- Producer
    my $list_id = $coll->insert( 'Some data stuff' );

    # Change the element of the list with the ID $list_id
    $updated = $coll->update( $list_id, $data_id, 'Some new data stuff' );

    #-- Consumer
    # Get data from a list with the ID $list_id
    @data = $coll->receive( $list_id );
    # or to obtain the data in the order they are received
    while ( my ( $list_id, $data ) = $coll->pop_oldest )
    {
        print "List '$list_id' had '$data'\n";
    }

To see a brief but working code example of the C<Redis::CappedCollection>
package usage look at the L</"An Example"> section.

To see a description of the used C<Redis::CappedCollection> data
structure (on Redis server) look at the L</"CappedCollection data structure">
section.

=head1 ABSTRACT

Redis::CappedCollection module provides the fixed sized collections that have
a auto-FIFO age-out feature.

=head1 DESCRIPTION

The module provides an object oriented API.
This makes a simple and powerful interface to these services.

The main features of the package are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Support the work with data structures on the Redis server.

=item *

Supports the automatic creation of capped collection, status monitoring,
updating the data set, obtaining consistent data from the collection,
automatic data removal, the classification of possible errors.

=item *

Simple methods for organizing producer and consumer clients.

=back

Capped collections are fixed sized collections that have a auto-FIFO
age-out feature (age out is based on the time of the corresponding inserted data).
With the built-in FIFO mechanism, you are not at risk of using
excessive disk space.
Capped collections keep data in their time the corresponding inserted data order
automatically (in the respective lists of data).
Capped collections automatically maintain insertion order for the data lists
in the collection.

You may insert new data in the capped collection.
If there is a list with the ID, the data is inserted into the existing list,
otherwise it is inserted into a new list.

You may update the existing data in the collection.

Once the space is fully utilized, newly added data will replace
the oldest data in the collection.
Limits are specified when the collection is created.

The collection does not allow deleting data.

Automatic Age Out:
If you know you want data to automatically "roll out" over time as it ages,
a capped collection can be an easier way to support than writing manual removal
via cron scripts.

=head2 CONSTRUCTOR

=head3 C<new( redis =E<gt> $server, ... )>

It generates a C<Redis::CappedCollection> object to communicate with
the Redis server and can be called as either a class method or an object method.
If invoked with no arguments the constructor C<new> creates and returns
a C<Redis::CappedCollection> object that is configured
to work with the default settings.

If invoked with the first argument being an object of C<Redis::CappedCollection>
class or L<Redis|Redis> class, then the new object connection attribute values
are taken from the object of the first argument. It does not create
a new connection to the Redis server.

C<new> optionally takes arguments. These arguments are in key-value pairs.

This example illustrates a C<new()> call with all the valid arguments:

    my $coll = Redis::CappedCollection->new(
        redis   => "$server:$port", # Default Redis local server and port
        name    => 'Some name', # If 'name' is not specified, it creates
                        # a new collection named as UUID.
                        # If specified, the work is done with
                        # a given collection or a collection collection is
                        # created with the specified name.
        size            => 100_000, # The maximum size, in bytes,
                        # of the capped collection data
                        # (Default 0 - no limit).
                        # Is set for the new collection.
        size_garbage    => 50_000, # The minimum size, in bytes,
                        # of the data to be released, if the size
                        # of the collection data after adding new data
                        # may exceed 'size'
                        # (Default 0 - additional data
                        # should not be released).
        max_datasize    => 1_000_000,   # Maximum size, in bytes, of the data.
                        # (Default 512MB).
        older_allowed   => 0, # Permission to add data with time is less
                        # than the data time of which was deleted from the list.
                        # (Default 0 - insert too old data is prohibited).
        );

Requirements for arguments C<name>, C<size>, are described in more detail
in the sections relating to the methods L</name>, L</size> .

If the value of C<name> is specified, the work is done with a given collection
or creates a new collection with the specified name.
Do not use the symbol C<':'> in C<name>.

The following examples illustrate other uses of the C<new> method:

    $coll = Redis::CappedCollection->new();
    my $next_coll = Redis::CappedCollection->new( $coll );

    my $redis = Redis->new( redis => "$server:$port" );
    $next_coll = Redis::CappedCollection->new( $redis );

An error will cause the program to halt (C<confess>) if an argument is not valid.

=head2 METHODS

An error will cause the program to halt (C<confess>) if an argument is not valid.

ATTENTION: In the L<Redis|Redis> module the synchronous commands throw an
exception on receipt of an error reply, or return a non-error reply directly.

=head3 C<insert( $data, $list_id, $data_id, $data_time )>

Adds data to the capped collection on the Redis server.

Data obtained in the first argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

Data is added to the existing list, if the second argument is specified
and the corresponding queue already exists.
Otherwise, the data is added to a new queue for which ID is the second argument.
ID in the second argument must be a non-empty string (if specified).
Do not use the symbol C<':'> in C<$list_id>.

If the second argument is not specified, the data is added to a new list
with automatically generated ID (UUID).

For a given data may be given the unique ID and the unique time.
If the ID is not specified (or an empty string), it will be automatically
generated in the form of sequential integer.
Data ID must be unique for a list of data.

Data ID must not be an empty string.

Time must be a non-negative number. If time is not specified, it will be
automatically generated as the result of a function call
C<Time::HiRes::gettimeofday> in a scalar context.


The following examples illustrate uses of the C<insert> method:

In a scalar context, the method returns the ID of the data list to which
you add the data.

    $list_id = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    $list_id = $coll->insert( 'Some data stuff' );

In a list context, the method returns the ID of the data list to which
your adding the data and the data ID corresponding to your data.

    ( $list_id, $data_id ) = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    ( $list_id, $data_id ) = $coll->insert( 'Some data stuff' );

=head3 C<update( $list_id, $data_id, $data )>

Updates the data in the queue identified by the first argument.
C<$list_id> must be a non-empty string.

The updated data ID given as the second argument.
The C<$data_id> must be a non-empty string.

New data should be included in the third argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

The following examples illustrate uses of the C<update> method:

    if ( $coll->update( $list_id, 0, 'Some new data stuff' ) )
    {
        print "Data updated successfully\n";
    }
    else
    {
        print "The data is not updated\n";
    }

Method returns true if the data is updated or false if the queue with
the given ID does not exist or is used an invalid data ID.

=head3 C<receive( $list_id, $data_id )>

If the C<$data_id> argument is not specified or is an empty string:

=over 3

=item *

In a list context, the method returns all the data from the list given by
the C<$list_id> identifier.
If the C<$data_id> argument is an empty string than it returns all data IDs and
data values of the data list.
Method returns an empty list if the list with the given ID does not exist.

=item *

In a scalar context, the method returns the length of the data list given by
the C<$list_id> identifier.

=back

If the C<$data_id> argument is specified:

=over 3

=item *

The method returns the specified element of the data list.
If the data with C<$data_id> ID does not exists, C<undef> is returned.

=back

C<$list_id> must be a non-empty string.
C<$data_id> must be a normal string.

The following examples illustrate uses of the C<receive> method:

    my @data = $coll->receive( $list_id );
    print "List '$list_id' has '$_'\n" foreach @data;
    # or
    my $list_len = $coll->receive( $list_id );
    print "List '$list_id' has '$list_len' item(s)\n";
    # or
    my $data = $coll->receive( $list_id, 0 );
    print "List '$list_id' has '$data' in 'zero' position\n";

=head3 C<pop_oldest>

The method is designed to retrieve the oldest data stored in the collection.

Returns a list of two elements.
The first element contains the identifier of the list from which the data was retrieved.
The second element contains the extracted data.
When retrieving data, it is removed from the collection.

If you perform a C</pop_oldest> on the collection, the data will always
be returned in order of the time corresponding inserted data.

Method returns an empty list if the collection does not contain any data.

The following examples illustrate uses of the C<pop_oldest> method:

    while ( my ( $list_id, $data ) = $coll->pop_oldest )
    {
        print "List '$list_id' had '$data'\n";
    }

=head3 C<validate>

The method is designed to obtain information on the status of the collection
and to see how much space an existing collection uses.

Returns a list of three elements:

=over 3

=item *

Size in bytes of all the data stored in all the collection lists.

=item *

Number of lists of data stored in a collection.

=item *

Number of data items stored in the collection.

=back

The following examples illustrate uses of the C<validate> method:

    my ( $length, $lists, $items ) = $coll->validate;
    print "An existing collection uses $length byte of data, ",
        "in $items items are placed in $lists lists\n";

=head3 C<exists( $list_id )>

The method is designed to test whether there is a list in the collection with
ID C<$list_id>.
Returns true if the list exists and false otherwise.

The following examples illustrate uses of the C<exists> method:

    print "The collection has '$list_id' list\n" if $coll->exists( 'Some_id' );

=head3 C<lists( $pattern )>

Returns a list of identifiers stored in a collection.
Returns all list IDs matching C<$pattern> if C<$pattern> is not empty.
C<$patten> must be a non-empty string.

Supported glob-style patterns:

=over 3

=item *

C<h?llo> matches C<hello>, C<hallo> and C<hxllo>

=item *

C<h*llo> matches C<hllo> and C<heeeello>

=item *

C<h[ae]llo> matches C<hello> and C<hallo>, but not C<hillo>

=back

Use \ to escape special characters if you want to match them verbatim.

The following examples illustrate uses of the C<lists> method:

    print "The collection has '$_' list\n" foreach $coll->lists;

Warning: consider C<lists> as a command that should only be used in production
environments with extreme care. It may ruin performance when it is executed
against large databases.
This command is intended for debugging and special operations.
Don't use C<lists> in your regular application code.

=head3 C<drop>

Use the C</drop> method to remove the entire collection.
Method removes all the structures on the Redis server associated with
the collection.
After using C</drop> you must explicitly recreate the collection.

Before use, make sure that the collection is not being used by other customers.

The following examples illustrate uses of the C<drop> method:

    $coll->drop;

Warning: consider C<drop> as a command that should only be used in production
environments with extreme care. It may ruin performance when it is executed
against large databases.
This command is intended for debugging and special operations.
Don't use C<drop> in your regular application code.

=head3 C<quit>

Ask the Redis server to close the connection.

The following examples illustrate uses of the C<quit> method:

    $jq->quit;

=head3 C<name>

The method of access to the C<name> attribute (collection ID).
The method returns the current value of the attribute.
The C<name> attribute value is used in the L<constructor|/CONSTRUCTOR>.

If the value of C<name> is not specified to the L<constructor|/CONSTRUCTOR>,
it creates a new collection ID as UUID.

=head3 C<size>

The method of access to the C<size> attribute - the maximum size, in bytes,
of the capped collection data (Default 0 - no limit).

The method returns the current value of the attribute.
The C<size> attribute value is used in the L<constructor|/CONSTRUCTOR>.

Is set for the new collection. Otherwise an error will cause the program to halt
(C<confess>) if the value is not equal to the value that was used when
a collection was created.

=head3 C<size_garbage>

The method of access to the C<size_garbage> attribute - the minimum size,
in bytes, of the data to be released, if the size of the collection data
after adding new data may exceed L</size> (Default 0 - additional data
should not be released).

The C<size_garbage> attribute is designed to reduce the release of memory
operations with frequent data changes.

The C<size_garbage> attribute value can be used in the L<constructor|/CONSTRUCTOR>.
The method returns and sets the current value of the attribute.

The C<size_garbage> value may be less than or equal to L</size>. Otherwise 
an error will cause the program to halt (C<confess>).

=head3 C<max_datasize>

The method of access to the C<max_datasize> attribute.

The method returns the current value of the attribute if called without arguments.

Non-negative integer value can be used to specify a new value to
the maximum size of the data introduced into the collection
(methods L</insert> and L</update>).

The C<max_datasize> attribute value is used in the L<constructor|/CONSTRUCTOR>
and operations data entry on the Redis server.

The L<constructor|/CONSTRUCTOR> uses the smaller of the values of 512MB and
C<maxmemory> limit from a C<redis.conf> file.

=head3 C<older_allowed>

The method of access to the C<older_allowed> attribute -
permission to add data with time less than the data time which was
deleted from the data list. Default 0 - insert too old data is prohibited.

The method returns the current value of the attribute.
The C<older_allowed> attribute value is used in the L<constructor|/CONSTRUCTOR>.

=head3 C<last_errorcode>

The method of access to the code of the last identified errors.

To see more description of the identified errors look at the L</DIAGNOSTICS>
section.

=head2 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=over

=item C<DEFAULT_SERVER>

Default Redis local server - C<'localhost'>.

=item C<DEFAULT_PORT>

Default Redis server port - 6379.

=item C<NAMESPACE>

Namespace name used keys on the Redis server - C<'Capped'>.

=back

=head2 DIAGNOSTICS

The method for the possible error to analyse: L</last_errorcode>.

A L<Redis|Redis> error will cause the program to halt (C<confess>).
In addition to errors in the L<Redis|Redis> module, detected errors are
L</EMISMATCHARG>, L</EDATATOOLARGE>, L</EMAXMEMORYPOLICY>, L</ECOLLDELETED>,
L</EDATAIDEXISTS>, L</EOLDERTHANALLOWED>.

All recognizable errors in C<Redis::CappedCollection> lead to
the installation of the corresponding value in the L</last_errorcode> and cause
an exception (C<confess>).
Unidentified errors cause an exception (L</last_errorcode> remains equal to 0).
The initial value of C<$@> is preserved.

The user has the choice:

=over 3

=item *

Use the module methods and independently analyze the situation without the use
of L</last_errorcode>.

=item *

Piece of code wrapped in C<eval {...};> and analyze L</last_errorcode>
(look at the L</"An Example"> section).

=back

In L</last_errortsode> recognizes the following:

=over 3

=item C<ENOERROR>

No error.

=item C<EMISMATCHARG>

This means that you didn't give the right argument to a C<new>
or to other L<method|/METHODS>.

=item C<EDATATOOLARGE>

This means that the data is too large.

=item C<ENETWORK>

This means that an error in connection to Redis server was detected.

=item C<EMAXMEMORYLIMIT>

This means that the command is not allowed when used memory > C<maxmemory>
in the C<redis.conf> file.

=item C<EMAXMEMORYPOLICY>

This means that you are using the C<maxmemory-police all*> in the C<redis.conf> file.

=item C<ECOLLDELETED>

This means that the system part of the collection was removed prior to use.

=item C<EREDIS>

This means that other Redis error message detected.

=item C<EDATAIDEXISTS>

This means that you are trying to insert data with an ID that is already in
the data list.

=item C<EOLDERTHANALLOWED>

This means that you are trying to insert the data with the time less than
the time of the data that has been deleted from the data list.

=back

=head2 An Example

The example shows a possible treatment for possible errors.

    #-- Common ---------------------------------------------------------
    use Redis::CappedCollection qw(
        DEFAULT_SERVER
        DEFAULT_PORT

        ENOERROR
        EMISMATCHARG
        EDATATOOLARGE
        ENETWORK
        EMAXMEMORYLIMIT
        EMAXMEMORYPOLICY
        ECOLLDELETED
        EREDIS
        );

    # A possible treatment for possible errors
    sub exception {
        my $coll    = shift;
        my $err     = shift;

        die $err unless $coll;
        if ( $coll->last_errorcode == ENOERROR )
        {
            # For example, to ignore
            return unless $err;
        }
        elsif ( $coll->last_errorcode == EMISMATCHARG )
        {
            # Necessary to correct the code
        }
        elsif ( $coll->last_errorcode == EDATATOOLARGE )
        {
            # You must use the control data length
        }
        elsif ( $coll->last_errorcode == ENETWORK )
        {
            # For example, sleep
            #sleep 60;
            # and return code to repeat the operation
            #return 'to repeat';
        }
        elsif ( $coll->last_errorcode == EMAXMEMORYLIMIT )
        {
            # For example, return code to restart the server
            #return 'to restart the redis server';
        }
        elsif ( $coll->last_errorcode == EMAXMEMORYPOLICY )
        {
            # For example, return code to reinsert the data
            #return "to reinsert look at $err";
        }
        elsif ( $coll->last_errorcode == ECOLLDELETED )
        {
            # For example, return code to ignore
            #return "to ignore $err";
        }
        elsif ( $coll->last_errorcode == EREDIS )
        {
            # Independently analyze the $err
        }
        elsif ( $coll->last_errorcode == EDATAIDEXISTS )
        {
            # For example, return code to reinsert the data
            #return "to reinsert with new data ID";
        }
        elsif ( $coll->last_errorcode == EOLDERTHANALLOWED )
        {
            # Independently analyze the situation
        }
        else
        {
            # Unknown error code
        }
        die $err if $err;
    }

    my ( $list_id, $coll, @data );

    eval {
        $coll = Redis::CappedCollection->new(
            redis   => DEFAULT_SERVER.':'.DEFAULT_PORT,
            name    => 'Some name',
            size    => 100_000,
            );
    };
    exception( $coll, $@ ) if $@;
    print "'", $coll->name, "' collection created.\n";
    print 'Restrictions: ', $coll->size, ' size, "\n";

    #-- Producer -------------------------------------------------------
    #-- New data

    eval {
        $list_id = $coll->insert(
            'Some data stuff',
            'Some_id',      # If not specified, it creates
                            # a new items list named as UUID
            );
        print "Added data in a list with '", $list_id, "' id\n" );

        # Change the "zero" element of the list with the ID $list_id
        if ( $coll->update( $list_id, 0, 'Some new data stuff' ) )
        {
            print "Data updated successfully\n";
        }
        else
        {
            print "The data is not updated\n";
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Consumer -------------------------------------------------------
    #-- Fetching the data

    eval {
        @data = $coll->receive( $list_id );
        print "List '$list_id' has '$_'\n" foreach @data;
        # or to obtain the data in the order they are received
        while ( my ( $list_id, $data ) = $coll->pop_oldest )
        {
            print "List '$list_id' had '$data'\n";
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Utility --------------------------------------------------------
    #-- Getting statistics

    my ( $length, $lists, $items );
    eval {
        ( $length, $lists, $items ) = $coll->validate;
        print "An existing collection uses $length byte of data, ",
            "in $items items are placed in $lists lists\n";

        print "The collection has '$list_id' list\n"
            if $coll->exists( 'Some_id' );
    };
    exception( $coll, $@ ) if $@;

    #-- Closes and cleans up -------------------------------------------

    eval {
        $coll->quit;

        # Before use, make sure that the collection
        # is not being used by other customers
        #$coll->drop;
    };
    exception( $coll, $@ ) if $@;

=head2 CappedCollection data structure

Using the currently selected database (default = 0).

While working on the Redis server creates and uses these data structures:

    #-- To store the collection status:
    # HASH    Namespace:status:Collection_id

For example:

    $ redis-cli
    redis 127.0.0.1:6379> KEYS Capped:status:*
    1) "Capped:status:89116152-C5BD-11E1-931B-0A690A986783"
    #      |      |                       |
    #  Namespace  |                       |
    #  Fixed symbol of a properties hash  |
    #                         Capped Collection id (UUID)
    ...
    redis 127.0.0.1:6379> HGETALL Capped:status:89116152-C5BD-11E1-931B-0A690A986783
    1) "size"                   # hash key
    2) "0"                      # the key value
    3) "length"                 # hash key
    4) "15"                     # the key value
    5) "lists"                  # hash key
    6) "1"                      # the key value
    7) "items"                  # hash key
    8) "1"                      # the key value
    9) "older_allowed"          # hash key
    10) "0"                     # the key value

    #-- To store the collection queue:
    # ZSET    Namespace:queue:Collection_id

For example:

    redis 127.0.0.1:6379> KEYS Capped:queue:*
    1) "Capped:queue:89116152-C5BD-11E1-931B-0A690A986783"
    #      |     |                       |
    #  Namespace |                       |
    #  Fixed symbol of a queue           |
    #                        Capped Collection id (UUID)
    ...
    redis 127.0.0.1:6379> ZRANGE Capped:queue:89116152-C5BD-11E1-931B-0A690A986783 0 -1 WITHSCORES
    1) "478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    2) "1348252575.6651001"|    |
    #           |          |    |
    #  Score: oldest data_time  |
    #                   Member: Data List id (UUID)
    ...

    #-- To store the CappedCollection data:
    # HASH    Namespace:I:Collection_id:DataList_id
    # HASH    Namespace:D:Collection_id:DataList_id
    # ZSET    Namespace:T:Collection_id:DataList_id

For example:

    redis 127.0.0.1:6379> KEYS Capped:?:*
    1) "Capped:I:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    2) "Capped:D:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    3) "Capped:T:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    #     |    |                     |                       |
    # Namespace|                     |                       |
    # Fixed symbol of a list of data |                       |
    #                    Capped Collection id (UUID)         |
    #                                                Data list id (UUID)
    ...
    redis 127.0.0.1:6379> HGETALL Capped:I:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    1) "next_data_id"           # hash key
    2) "1"                      # the key value
    3) "last_removed_time"      # hash key
    4) "0"                      # the key value
    5) "oldest_time"            # hash key
    6) "1348252575.5906"        # the key value
    7) "oldest_data_id"         # hash key
    8) "0"                      # the key value

    redis 127.0.0.1:6379> HGETALL Capped:D:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    1) "0"                      # hash key: Data id
    2) "Some stuff"             # the key value: Data

    redis 127.0.0.1:6379> ZRANGE Capped:T:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783 0 -1 WITHSCORES
    1) "0" ---------------+
    2) "1348252575.5906"  |
    #           |         |
    #   Score: data_time  |
    #              Member: Data id
    ...

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.010 or better. The Redis::CappedCollection module depends on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Redis::CappedCollection :

   Data::UUID
   Digest::SHA1
   Mouse
   Params::Util
   Redis

The Redis::CappedCollection module has the following optional dependencies:

   Test::Distribution
   Test::Exception
   Test::Kwalitee
   Test::Perl::Critic
   Test::Pod
   Test::Pod::Coverage
   Test::RedisServer
   Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

The installation of the missing dependencies can either be accomplished
through your OS package manager or through CPAN (or downloading the source
for all dependencies and compiling them manually).

=head1 BUGS AND LIMITATIONS

Need a Redis server version 2.6 or higher as module uses Redis Lua scripting.

The use of C<maxmemory-police all*> in the C<redis.conf> file could lead to
a serious (but hard to detect) problem as Redis server may delete
the collection element. Therefore the C<Redis::CappedCollection> does not work with
mode C<maxmemory-police all*> in the C<redis.conf>.

Full name of some Redis keys may not be known at the time of the call
the Redis Lua script (C<'EVAL'> or C<'EVALSHA'> command).
So the Redis server may not be able to correctly forward the request
to the appropriate node in the cluster.

We strongly recommend using the option C<maxmemory> in the C<redis.conf> file if
the data set may be large.

The C<Redis::CappedCollection> module was written, tested, and found working
on recent Linux distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

The basic operation of the Redis::CappedCollection package module:

L<Redis::CappedCollection|Redis::CappedCollection> - Object interface to create
a collection, addition of data and data manipulation.

L<Redis|Redis> - Perl binding for Redis database.

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
