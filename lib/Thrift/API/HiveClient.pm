use strict;
use warnings;
package Thrift::API::HiveClient;
# ABSTRACT: A Hadoop Hive client using the Thrift API
use Moo;
use Carp;
use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;
use Thrift::API::HiveClient::Service::ThriftHive;

=attr host

The host on which the hiveserver process is running. Should be a
valid hostname or IP address. I<required>, I<immutable>

=cut
has host => ( is => 'ro' );

=attr port

The TCP port on which the hiveserver process is listening. Should be
a valid port number. I<required>, I<immutable>

=cut
has port => ( is => 'ro' );

# These exist to make testing with various other
# Thrift Implementation classes easier, eventually.
has _socket => ( is => 'rwp' );
has _transport => ( is => 'rwp' );
has _protocol => ( is => 'rwp' );
has _client => ( is => 'rwp' );

# setters implied by the 'rwp' mode on the attrs above.
# having to write these these annoys me. Time to write a MooX module...
sub _set_socket { $_[0]->{_socket} = $_[1] }
sub _set_transport { $_[0]->{_transport} = $_[1] }
sub _set_protocol { $_[0]->{_protocol} = $_[1] }
sub _set_client { $_[0]->{_client} = $_[1] }

# after constructon is complete, initialize any attributes that
# weren't set in the constructor.
sub BUILD {
  my ($self) = @_;

  $self->_set_socket(
     Thrift::Socket->new( $self->host, $self->port )
  ) unless $self->_socket;

  $self->_set_transport(
    Thrift::BufferedTransport->new( $self->_socket )
  ) unless $self->_transport;

  $self->_set_protocol(
    Thrift::BinaryProtocol->new( $self->_transport )
  ) unless $self->_protocol;

  $self->_set_client(
    Thrift::API::HiveClient::Service::ThriftHiveClient->new( $self->_protocol )
  ) unless $self->_client;
}

=method new( ... )

Standard object constructor. Arguments are the attributes described above. For example:

  my $cli = Thrift::API::HiveClient->new( host => 'localhost', port => 10000 );

=cut

=method connect( )

Connect to the configured hiveserver.

=cut
sub connect {
  my ($self) = @_;
  $self->_transport->open;
}

# when the user calls a method on an object of this class, see if that method exists
# on the ThriftHiveClient object. If so, create a sub that calls that method on the
# client object. If not, die horribly.
sub AUTOLOAD {
    my ($self) = @_;
    (my $meth = our $AUTOLOAD) =~ s/.*:://;
    return if $meth eq 'DESTROY';
    no strict 'refs';
    if ( $self->_client->can($meth) ) {
      *$AUTOLOAD = sub { shift->_client->$meth( @_ ) };
      goto &$AUTOLOAD;
    }
    croak "No such method exists: $AUTOLOAD";
}

1 && q{this is a terrible kludge}; # truth
__END__

=head1 DESCRIPTION

B<THIS CODE IS ALPHA-QUALITY, EXPERIMENTAL, AND LIKELY FLAMMABLE>

That said, I decided it would be nice to make it easy to connect to a Hadoop Hive server via
its Thrift interface and that's what this module provides. It tries to keep the gory details
from you and make things as simple as possible. The majority of the code in this dist was
generated by the thrift compiler, but is hidden behind the friendly facade of the module
whose documentation you are reading now. Enjoy, and please feel free to send bug reports
and/or patches!

=head1 METHODS FROM ThriftHive

These are the methods exposed by the Thrift API. The HiveClient object simply acts
as a proxy for all the methods available on the (underlying) ThriftHiveClient object.

However, for your convenience, all these methods are documented here, as well as I can.

=head2 execute( $hql )

The given HQL statement is sent to the hiveserver and executed. The results can be
retrieved by using the fetch* methods.

  $cli->execute('select * from foo');

=head2 fetchOne( )

=head2 fetchN( $num )

=head2 fetchAll( )

All results from a previous call to execute() are returned as an array

  my $res = $cli->fetchAll();

=head2 getSchema( )

=head2 getThriftSchema( )

=head2 getClusterStatus( )

Returns an object (blessed hash) showing some of the status of the Hadoop cluster.

=head2 getQueryPlan( )

=head2 clean( )

=head1 METHODS FROM ThriftHiveMetastore

=head2 create_database( $db_name )

=head2 get_database( $db_name )

=head2 drop_database( $db_name, $drop_data )

=head2 get_databases( $pattern )

=head2 get_all_databases( )

=head2 alter_database( $db_name, $db )

=head2 get_type( $name )

=head2 create_type( $type )

=head2 drop_type( $type )

=head2 get_type_all( $name )

=head2 get_fields( $db_name, $table_name )

Returns an array of FieldSchema objects with info about the fields in the given
table in the given database.

  my $fields = $cli->get_fields( 'default', 'foo' );

=head2 get_schema( $db_name, $table_name )

=head2 create_table( $tbl )

=head2 drop_table( $db_name, $tbl_name, $delete_data )

=head2 get_tables( $db_name, $pattern )

=head2 get_all_tables( $db_name )

=head2 get_table( $db_name, $tbl_name )

=head2 alter_table( $db_name, $tbl_name, $new_tbl )

=head2 add_partition( $new_part )

=head2 append_partition( $db_name, $tbl_name, $part_vals )

=head2 append_partition_by_name( $db_name, $tbl_name, $part_name )

=head2 drop_partition( $db_name, $tbl_name, $part_vals, $delete_data )

=head2 drop_partition_by_name( $db_name, $tbl_name, $part_name, $delete_data )

=head2 get_partition( $db_name, $tbl_name, $part_vals )

=head2 get_partition_with_auth( $db_name, $tbl_name, $part_vals, $usr_name, $grp_names )

=head2 get_partition_by_name( $db_name, $tbl_name, $part_name )

=head2 get_partitions( $db_name, $tbl_name, $max_parts )

=head2 get_partitions_with_auth( $db_name, $tbl_name, $max_parts, $usr_name, $grp_names )

=head2 get_partition_names( $db_name, $tbl_name, $max_parts )


=head2 get_partitions_ps

=head2 get_partitions_ps_with_auth

=head2 get_partition_names_ps

=head2 get_partitions_by_filter

=head2 alter_partition

=head2 get_config_value( $name, $default )

=head2 partition_name_to_vals

=head2 partition_name_to_spec

=head2 add_index

=head2 alter_index

=head2 drop_index_by_name

=head2 get_index_by_name

=head2 get_indexes

=head2 get_index_names

=head2 create_role

=head2 drop_role

=head2 get_role_names

=head2 grant_role

=head2 revoke_role

=head2 list_roles

=head2 get_privilege_set

=head2 list_privileges

=head2 grant_privileges

=head2 revoke_privileges

=head2 get_delegation_token

=head2 get_delegation_token_with_signature

=head2 renew_delegation_token

=head2 cancel_delegation_token



=head1 METHODS FROM FacebookService



=head2 getName

=head2 getVersion

=head2 getStatus

=head2 getStatusDetails

=head2 getCounters

=head2 getCounter

=head2 setOption

=head2 getOption

=head2 getOptions

=head2 getCpuProfile

=head2 aliveSince

=head2 reinitialize

=head2 shutdown


