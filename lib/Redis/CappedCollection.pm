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
    );

#-- load the modules -----------------------------------------------------------

# Modules
use Mouse;
use Mouse::Util::TypeConstraints;
use Carp;
use Redis;
use Data::UUID;
use Digest::SHA1    qw( sha1_hex );
use List::Util      qw( min );
use Params::Util    qw( _NONNEGINT _STRING _INSTANCE );

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
    };

my @ERROR = (
    'No error',
    'Mismatch argument',
    'Data is too large',
    'Error in connection to Redis server',
    "Command not allowed when used memory > 'maxmemory'",
    'Data was removed by maxmemory-policy',
    'Collection was removed prior to use',
    'Redis error message',
    );

my $uuid = new Data::UUID;

my %lua_script_body;

$lua_script_body{insert} =
<<"INSERT" ;
local queue_key     = KEYS[1]
local status_key    = KEYS[2]
local list_key      = KEYS[3]
local excess_lists  = KEYS[4]
local id            = ARGV[1]
local size          = ARGV[2] + 0
local data          = ARGV[3]
local size_garbage  = ARGV[4] + 0
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', list_key )
local data_index    = 0
if status_exist == 1 then
    if size > 0 then
        if ( redis.call( 'HGET', status_key, 'length' ) + #data ) > size then
            local size_limit = size - size_garbage
            while ( redis.call( 'HGET', status_key, 'length' ) + #data ) > size_limit do
                local excess_id = redis.call( 'LPOP', queue_key )
                local excess_list_key = excess_lists..':'..excess_id
                local excess_data = redis.call( 'LPOP', excess_list_key )
                local new_excess_list_exist = redis.call( 'EXISTS', excess_list_key )
                if new_excess_list_exist == 0 then
                    redis.call( 'HINCRBY', status_key, 'lists', -1 )
                end
                if #excess_data > 0 then
                    redis.call( 'HINCRBY', status_key, 'length', -#excess_data )
                end
            end
        end
    end
    if list_exist == 0 then
        redis.call( 'HINCRBY', status_key, 'lists', 1 )
    end
    if #data > 0 then
        redis.call( 'HINCRBY', status_key, 'length', #data );
    end
    data_index = redis.call( 'RPUSH', list_key, data ) - 1;
    redis.call( 'RPUSH', queue_key, id );
end
return { status_exist, data_index }
INSERT

$lua_script_body{update} =
<<"UPDATE" ;
local queue_key     = KEYS[1]
local status_key    = KEYS[2]
local list_key      = KEYS[3]
local excess_lists  = KEYS[4]
local size          = ARGV[1] + 0
local index         = ARGV[2] + 0
local data          = ARGV[3]
local size_garbage  = ARGV[4] + 0
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', list_key )
local ret           = false
if status_exist == 1 then
    if list_exist == 1 then
        if index < redis.call( 'LLEN', list_key ) then
            local cur_data = redis.call( 'LINDEX', list_key, index )
            if size > 0 then
                local size_delta = #data - #cur_data
                if ( redis.call( 'HGET', status_key, 'length' ) + size_delta ) > size then
                    local size_limit = size - size_garbage
                    while ( redis.call( 'HGET', status_key, 'length' ) + size_delta ) > size_limit do
                        local excess_id         = redis.call( 'LPOP', queue_key )
                        local excess_list_key   = excess_lists..':'..excess_id
                        local excess_data       = redis.call( 'LPOP', excess_list_key )
                        if excess_list_key == list_key then
                            index = index - 1
                        end
                        local new_excess_list_exist = redis.call( 'EXISTS', excess_list_key )
                        if new_excess_list_exist == 0 then
                            redis.call( 'HINCRBY', status_key, 'lists', -1 )
                        end
                        if #excess_data > 0 then
                            redis.call( 'HINCRBY', status_key, 'length', -#excess_data )
                        end
                    end
                end
            end
            if index >= 0 then
                ret = redis.call( 'LSET', list_key, index, data )
                if ret then
                    redis.call( 'HINCRBY', status_key, 'length', #data - #cur_data );
                end
            end
        end
    end
end
return { status_exist, ret }
UPDATE

$lua_script_body{pop_oldest} =
<<"POP_OLDEST" ;
local queue_key         = KEYS[1]
local status_key        = KEYS[2]
local excess_lists      = KEYS[3]
local queue_exist       = redis.call( 'EXISTS', queue_key )
local status_exist      = redis.call( 'EXISTS', status_key )
local excess_list_exist = 0
local excess_id         = false
local excess_data       = false
if queue_exist == 1 and status_exist == 1 then
    excess_id               = redis.call( 'LPOP', queue_key )
    local excess_list_key   = excess_lists..':'..excess_id
    excess_list_exist       = redis.call( 'EXISTS', excess_list_key )
    if excess_list_exist == 1 then
        excess_data = redis.call( 'LPOP', excess_list_key )
        local new_excess_list_exist = redis.call( 'EXISTS', excess_list_key )
        if new_excess_list_exist == 0 then
            redis.call( 'HINCRBY', status_key, 'lists', -1 )
        end
        redis.call( 'HINCRBY', status_key, 'length', -#excess_data )
    end
end
return { queue_exist, status_exist, excess_list_exist, excess_id, excess_data }
POP_OLDEST

$lua_script_body{validate} =
<<"VALIDATE" ;
local queue_key     = KEYS[1]
local status_key    = KEYS[2]
local status_exist  = redis.call( 'EXISTS', status_key )
if status_exist == 1 then
    local length = redis.call( 'HGET', status_key, 'length' )
    local lists  = redis.call( 'HGET', status_key, 'lists' )
    local llen   = redis.call( 'LLEN', queue_key )
    return { status_exist, length, lists, llen }
end
return { status_exist, false, false, false }
VALIDATE

$lua_script_body{drop} =
<<"DROP" ;
local queue_key     = KEYS[1]
local status_key    = KEYS[2]
local lists_key     = KEYS[3]
return redis.call( 'DEL', queue_key, status_key, unpack( redis.call( 'KEYS', lists_key..':*' ) ) )
DROP

$lua_script_body{verify_collection} =
<<"VERIFY_COLLECTION" ;
local key   = KEYS[1]
local size  = ARGV[1]
local exist = redis.call( 'EXISTS', key )
if exist == 1 then
    local vals = redis.call( 'HMGET', key, 'size' )
    size = vals[1]
else
    redis.call( 'HMSET', key, 'size', size, 'length', 0, 'lists', 0 )
end
return { exist, size }
VERIFY_COLLECTION

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
    ;

subtype __PACKAGE__.'::NonEmptStr',
    as 'Str',
    where { $_ ne '' },
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

#    unless ( eval { $self->_redis->script( 'EXISTS', 'ffffffffffffffffffffffffffffffffffffffff' ) } )
    my ( $major, $minor ) = $self->_redis->info->{redis_version} =~ /^(\d+)\.(\d+)/;
    if ( $major < 2 or ( $major == 2 and $minor <= 4 ) )
    {
        $self->_set_last_errorcode( EREDIS );
        confess "Need a Redis server version 2.6 or higher";
    }

    $self->_maxmemory_policy( ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory-policy' ) )[1] );
    $self->_maxmemory(        ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory'        ) )[1] );
    $self->max_datasize( min $self->_maxmemory, $self->max_datasize )
        if $self->_maxmemory;

    $self->_queue_key(  NAMESPACE.':queue:'.$self->name );
    $self->_status_key( NAMESPACE.':status:'.$self->name );
    $self->_lists_key(  NAMESPACE.':L:'.$self->name );

    $self->_verify_collection;
}

#-- public attributes ----------------------------------------------------------

has 'name' => (
    is          => 'ro',
    clearer     => '_clear_name',
    isa         => __PACKAGE__.'::NonEmptStr',
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
                        $self->size_garbage <= $self->size || $self->_throw( EMISMATCHARG );
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
                                || $self->_throw( EMISMATCHARG );
                        }
                    },
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

foreach my $attr_name ( qw( _maxmemory_policy _queue_key _status_key _lists_key ) )
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
    my $id          = shift // $uuid->create_str;

    $data                                           // $self->_throw( EMISMATCHARG );
    _STRING( $id )                                  // $self->_throw( EMISMATCHARG );
    ( defined( _STRING( $data ) ) or $data eq '' )  || $self->_throw( EMISMATCHARG );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )  || $self->_throw( EMISMATCHARG ) );
    ( $data_len <= $self->max_datasize )            || $self->_throw( EDATATOOLARGE );

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $data_index ) = $self->_call_redis(
        $self->_lua_script_cmd( 'insert' ),
        4,
        $self->_queue_key,
        $self->_status_key,
        $self->_data_list_name( $id ),
        $self->_lists_key,
        $id,
        $self->size,
        $data,
        $self->size_garbage,
        );

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return wantarray ? ( $id, $data_index ) : $id;
}

sub update {
    my $self        = shift;
    my $id          = shift;
    my $index       = shift;
    my $data        = shift;

    $data                                           // $self->_throw( EMISMATCHARG );
    _STRING( $id )                                  // $self->_throw( EMISMATCHARG );
    ( defined( _STRING( $data ) ) or $data eq '' )  || $self->_throw( EMISMATCHARG );
    _NONNEGINT( $index )                            // $self->_throw( EMISMATCHARG );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )  || $self->_throw( EMISMATCHARG ) );
    ( $data_len <= $self->max_datasize )            || $self->_throw( EDATATOOLARGE );

    my ( $ret, $status_exist );
    $self->_set_last_errorcode( ENOERROR );

    ( $status_exist, $ret ) = $self->_call_redis(
        $self->_lua_script_cmd( 'update' ),
        4,
        $self->_queue_key,
        $self->_status_key,
        $self->_data_list_name( $id ),
        $self->_lists_key,
        $self->size,
        $index,
        $data,
        $self->size_garbage,
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
    my $id          = shift;
    my $index       = shift;

    _STRING( $id ) // $self->_throw( EMISMATCHARG );

    $self->_set_last_errorcode( ENOERROR );

    if ( defined $index )
    {
        _NONNEGINT( $index ) // $self->_throw( EMISMATCHARG );
        return $self->_call_redis( 'LRANGE', $self->_data_list_name( $id ), $index, $index );
    }
    else
    {
        if ( wantarray )
        {
            return $self->_call_redis( 'LRANGE', $self->_data_list_name( $id ), 0, -1 );
        }
        else
        {
            return $self->_call_redis( 'LLEN', $self->_data_list_name( $id ) );
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
            3,
            $self->_queue_key,
            $self->_status_key,
            NAMESPACE.':L:'.$self->name
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
        2,
        $self->_queue_key,
        $self->_status_key,
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
    my $id          = shift;

    _STRING( $id ) // $self->_throw( EMISMATCHARG );

    $self->_set_last_errorcode( ENOERROR );

    return $self->_call_redis( 'EXISTS', $self->_data_list_name( $id ) );
}

sub lists {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    return map { ( $_ =~ /:([^:]+)$/ )[0] } $self->_call_redis( 'KEYS', $self->_data_list_name( '*' ) );
}

sub drop {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    my $ret = $self->_call_redis(
        $self->_lua_script_cmd( 'drop' ),
        3,
        $self->_queue_key,
        $self->_status_key,
        $self->_lists_key,
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

    if ( !$self->_lua_scripts->{ $name } )
    {
        $self->_lua_scripts->{ $name } = sha1_hex( $lua_script_body{ $name } );
        return( 'EVAL', $lua_script_body{ $name } );
    }
    elsif ( $self->_maxmemory_policy !~ /^all/ )
    {
        return( 'EVALSHA', $self->_lua_scripts->{ $name } );
    }
    else
    {
        return( 'EVAL', $lua_script_body{ $name } );
    }
}

sub _data_list_name {
    my $self        = shift;
    my $id          = shift;

    return $self->_lists_key.':'.$id;
}

sub _verify_collection {
    my $self    = shift;

    my $key = NAMESPACE.':status:'.$self->name;
    $self->_set_last_errorcode( ENOERROR );

    my ( $key_exist, $size ) = $self->_call_redis(
        $self->_lua_script_cmd( 'verify_collection' ),
        1,
        $key,
        $self->size || 0,
        );
    if ( $key_exist )
    {
        $self->_set_size( $size ) unless $self->size;
        $size == $self->size or $self->_throw( EMISMATCHARG );
    }
}

sub _throw {
    my $self    = shift;
    my $err     = shift;

    $self->_set_last_errorcode( $err );
    confess $ERROR[ $err ];
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

Redis::CappedCollection - The fixed sized collections that have
a auto-FIFO age-out feature.

=head1 VERSION

This documentation refers to C<Redis::CappedCollection> version 0.01

=head1 SYNOPSIS

    #-- Common
    use Redis::CappedCollection qw( DEFAULT_SERVER DEFAULT_PORT )

    my $server = DEFAULT_SERVER.':'.DEFAULT_PORT;
    my $coll = Redis::CappedCollection->new( redis => $server );

    #-- Producer
    my $id = $coll->insert( 'Some data stuff' );

    # Change the zero element of the list with the ID $id
    $id = $coll->update( $id, 0, 'Some new data stuff' );

    #-- Consumer
    # Get data from a list with the ID $id
    @data = $coll->receive( $id );
    # or to obtain the data in the order they are received
    while ( my ( $id, $data ) = $coll->pop_oldest )
    {
        print "List '$id' had '$data'\n";
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
updating the data set, obtaining a consistent data from the collection,
automatic data remove, the classification of possible errors.

=item *

Simple methods for organizing producer and consumer clients.

=back

Capped collections are fixed sized collections that have a auto-FIFO
age-out feature (age out is based on insertion order).
With the built-in FIFO mechanism, you are not at risk of using
excessive disk space.
Capped collections keep data in their insertion order automatically
(in the respective lists of data).
Capped collections automatically maintain insertion order for the data lists
in the collection.
The collection contains the lists containing the data in the order they
are received.

You may insert new data in the capped collection.
If there is a list with the ID, the data is inserted into the end of the list,
or to a new list.

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
        name    => 'Some name', # If 'name' not specified, it creates
                        # a new collection named as UUID.
                        # If specified, the work is done with
                        # a given collection or create a collection
                        # with the specified name.
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
        );

Requirements for arguments C<name>, C<size>, described in more detail
in the sections relating to the methods L</name>, L</size> .

If the value of C<name> specified, the work is done with a given collection
or create a collection with the specified name.

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

=head3 C<insert( $data, $id )>

Adds data to the capped collection on the Redis server.

Data obtained in the first argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

Data is added to the existing list, if the second argument is specified
and the corresponding queue already exists.
Otherwise, the data is added to a new queue for which ID is the second argument.
ID in the second argument must be a non-empty string (if specified).
If the second argument is not specified, the data is added to a new list
with automatically generated ID (UUID).

The following examples illustrate uses of the C<insert> method:

In a scalar context, the method returns the ID of the data list to which
you add the data.

    $id = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    $id = $coll->insert( 'Some data stuff' );

In a list context, the method returns the ID of the data list to which
you add the data and the index position to which data is added after
the insert operation.

    ( $id, $index ) = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    ( $id, $index ) = $coll->insert( 'Some data stuff' );

In this case, the data list has length of C<$index + 1> items.

=head3 C<update( $id, $index, $data )>

Updates the data in the queue identified by the first argument.
ID must be a non-empty string.

Position updated data given as the second argument.
Indexing position starts with 0.
The C<$index> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

Data obtained in the third argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

The following examples illustrate uses of the C<update> method:

    if ( $coll->update( $id, 0, 'Some new data stuff' ) )
    {
        print "Data updated successfully\n";
    }
    else
    {
        print "The data is not updated\n";
    }

Method returns true if the data is updated or false if the queue with
the given ID does not exist or is used an invalid index.

=head3 C<receive( $id, $index )>

If the C<$index> argument is not specified:

In a list context, the method returns all the data from the list given by
the C<$id> identifier.
Data is returned in the order they are received in the list.
Method returns an empty list if the list with the given id does not exist.

In a scalar context, the method returns the length of the data list given by
the C<$id> identifier.

If the C<$index> argument is specified:

The method returns the specified element of the list. The offsets
are zero-based indexes, with 0 being the first element
of the list, 1 being the next element and so on.
Out of range indexes will not produce an error. For example, if C<$index>
is larger than the end of the list, an empty list is returned.

C<$id> must be a non-empty string.

The C<$index> must be a non-negative integer (of any length).
That is, a positive integer, or zero.

The following examples illustrate uses of the C<receive> method:

    my @data = $coll->receive( $id );
    print "List '$id' has '$_'\n" foreach @data;
    # or
    my $list_len = $coll->receive( $id );
    print "List '$id' has '$list_len' item(s)\n";
    # or
    my $data = $coll->receive( $id, 0 );
    print "List '$id' has '$data' in zero position\n";

=head3 C<pop_oldest>

The method is designed to retrieve the oldest data stored in the collection.

Returns a list of two elements.
The first element contains the identifier of the list from which to retrieve data.
The second element contains the extracted data.
When retrieving data, they are removed from the collection.

If you perform a C</pop_oldest> on the collection, the data will always
be returned in insertion order.

Method returns an empty list if the collection does not contain any data

The following examples illustrate uses of the C<pop_oldest> method:

    while ( my ( $id, $data ) = $coll->pop_oldest )
    {
        print "List '$id' had '$data'\n";
    }

=head3 C<validate>

The method is designed to obtain information on the status of the collection
and to see how much space an existing collection uses.

Returns a list of three elements:

=over 3

=item *

Size in bytes of all the data stored in all the lists collection.

=item *

Number of lists of data stored in a collection.

=item *

Number of data items stored in the collection.

=back

The following examples illustrate uses of the C<validate> method:

    my ( $length, $lists, $items ) = $coll->validate;
    print "An existing collection uses $length byte of data, ",
        "in $items items are placed in $lists lists\n";

=head3 C<exists( $id )>

The method is designed to test whether there is a list of the collection with
ID C<$id>.
Returns true if the list exists and false otherwise.

The following examples illustrate uses of the C<exists> method:

    print "The collection has '$id' list\n" if $coll->exists( 'Some_id' );

=head3 C<lists>

Returns a list of identifiers list data stored in a collection.

The following examples illustrate uses of the C<lists> method:

    print "The collection has '$_' list\n" foreach $coll->lists;

=head3 C<drop>

Use the C</drop> method to the removal of the entire collection.
Method removes all the structures on the Redis server associated with
the collection.
After the C</drop> you must explicitly recreate the collection.

Before use, make sure that the collection is not being used by other customers.

The following examples illustrate uses of the C<drop> method:

    $coll->drop;

=head3 C<quit>

Ask the Redis server to close the connection.

The following examples illustrate uses of the C<quit> method:

    $jq->quit;

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

=head3 C<last_errorcode>

The method of access to the code of the last identified errors.

To see more description of the identified errors look at the L</DIAGNOSTICS>
section.

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
after adding new data may exceed 'size' (Default 0 - additional data
should not be released).

The C<size_garbage> attribute is designed to reduce the release of memory
operations with frequent data changes.

The C<size_garbage> attribute value can be used in the L<constructor|/CONSTRUCTOR>.
The method returns and sets the current value of the attribute.

The C<size_garbage> value may be less than or equal to C<size>. Otherwise 
an error will cause the program to halt (C<confess>).

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
In addition to errors in the L<Redis|Redis> module detected errors
L</EMISMATCHARG>, L</EDATATOOLARGE>, L</EMAXMEMORYPOLICY>, L</ECOLLDELETED>.
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

This means that the command not allowed when used memory > C<maxmemory>
in the C<redis.conf> file.

=item C<EMAXMEMORYPOLICY>

This means that the collection element was removed by C<maxmemory-policy>
in the C<redis.conf> file.

=item C<ECOLLDELETED>

This means that the system part of the collection was removed prior to use.

=item C<EREDIS>

This means that other Redis error message detected.

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
            # For example, return code to reinser the data
            #return "to recreate look at $err";
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
        else
        {
            # Unknown error code
        }
        die $err if $err;
    }

    my ( $id, $coll, @data );

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
        $id = $coll->insert(
            'Some data stuff',
            'Some_id',      # If not specified, it creates
                            # a new items list named as UUID
            );
        print "Added data in a list with '", $id, "' id\n" );

        # Change the zero element of the list with the ID $id
        if ( $coll->update( $id, 0, 'Some new data stuff' ) )
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
        @data = $coll->receive( $id );
        print "List '$id' has '$_'\n" foreach @data;
        # or to obtain the data in the order they are received
        while ( my ( $id, $data ) = $coll->pop_oldest )
        {
            print "List '$id' had '$data'\n";
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

        print "The collection has '$id' list\n"
            if $coll->exists( 'Some_id' );

        print "Collection '", $coll->name, "' has '$_' list\n"
            foreach $coll->lists;
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

    #-- To store the collection queue:
    # LIST    Namespace:queue:Collection_id

For example:

    $ redis-cli
    redis 127.0.0.1:6379> KEYS Capped:queue:*
    1) "Capped:queue:89116152-C5BD-11E1-931B-0A690A986783"
    #      |     |                       |
    #  Namespace |                       |
    #  Fixed symbol of a queue           |
    #                        Capped Collection id (UUID)
    ...
    redis 127.0.0.1:6379> LRANGE Capped:queue:89116152-C5BD-11E1-931B-0A690A986783 0 -1
    1) "478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    #                    |
    #          Oldest data list id (UUID)
    ...

    #-- To store the CappedCollection data:
    # LIST    Namespace:L:Collection_id:DataList_id

For example:

    $ redis-cli
    redis 127.0.0.1:6379> KEYS Capped:L:*
    1) "Capped:L:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    #     |    |                     |                       |
    # Namespace|                     |                       |
    # Fixed symbol of a list of data |                       |
    #                    Capped Collection id (UUID)         |
    #                                                Data list id (UUID)
    ...
    redis 127.0.0.1:6379> LRANGE Capped:478B9C84-C5B8-11E1-A2C5-D35E0A986783 0 -1
    1) "Some data stuff"
    #          |
    # Oldest data from a list of data
    ...

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.010 or better. The Redis::CappedCollection module depend on other
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

Full name of some Redis keys may not be known at the time of the call
the Redis lua script (C<'EVAL'> or C<'EVALSHA'> command).
So the Redis server may not be able to correctly forward the request
to the appropriate node in the cluster.

The use of C<maxmemory-police all*> in the C<redis.conf> file could lead to
a serious (but hard to detect) problem as Redis server may delete
the collection element. It could also clear the script cache. In such case
we use the Redis server C<'EVAL'> command instead C<'EVALSHA'>.

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
