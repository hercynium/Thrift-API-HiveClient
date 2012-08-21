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

=method connect
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

=head1 THRIFT API METHODS

These are the methods exposed by the Thrift API. The HiveClient object simply acts
as a proxy for all the methods available on the (underlying) ThriftHiveClient object.

However, for your convenience, all these methods are documented here, as well as I can.

=head2 execute( $hql )

The given HQL statement is sent to the hiveserver and executed. The results can be
retrieved by using the fetch* methods.

  $cli->execute('select * from foo');

=head2 fetchAll()

All results from a previous call to execute() are returned as an array

  my $res = $cli->fetchAll();

=head2 getClusterStatus()

Returns an object (blessed hash) showing some of the status of the Hadoop cluster.

=head2 get_fields( $db_name, $tbl_name );

Returns an array of FieldSchema objects with info about the fields in the given
table in the given database.

  my $tbl_schema = $cli->get_fields( 'default', 'foo' );


