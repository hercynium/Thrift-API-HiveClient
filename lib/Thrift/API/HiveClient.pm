use strict;
use warnings;
package Thrift::API::HiveClient;
use Moo;
use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::BinaryProtocol;

# contains package Thrift::API::HiveClient::Service::ThriftHiveClient 
use Thrift::API::HiveClient::Service::ThriftHive;

use Data::Dumper;

has host => ( is => 'ro' );
has port => ( is => 'ro' );

# private attributes
has _socket => ( is => 'rwp' );
has _transport => ( is => 'rwp' );
has _protocol => ( is => 'rwp' );
has _client => ( is => 'rwp' );

# these annoy me. Time to write another MooX module...
sub _set_socket { $_[0]->{_socket} = $_[1] }
sub _set_transport { $_[0]->{_transport} = $_[1] }
sub _set_protocol { $_[0]->{_protocol} = $_[1] }
sub _set_client { $_[0]->{_client} = $_[1] }

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

sub connect {
  my ($self) = @_;
  $self->_transport->open;
}

sub AUTOLOAD {
    my ($self) = @_;
    (my $meth = our $AUTOLOAD) =~ s/.*:://;
    no strict 'refs';
    if ( $self->_client->can($meth) ) {
      *$AUTOLOAD = sub { shift->_client->$meth( @_ ) };
      goto &$AUTOLOAD;
    }
}


1;
