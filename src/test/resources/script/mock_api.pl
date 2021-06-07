#!/usr/bin/perl
use strict;
use warnings;
use utf8;
use feature ':5.16';
use Mojolicious::Lite -signatures;
use Data::Dumper;
use JSON::Parse 'parse_json';
my $payload = { data => { age => 25, salary => 25000 } };
{
    post '/fireshots/getInfo' => sub($c) {
        my $text = parse_json( $c->req->body );
        $payload->{data}->{full_name} =
          $text->{'first_name'} . $text->{'last_name'};
        $payload->{data}->{country} = $text->{'country'};
        $c->render( json => $payload );
    };
    get '/fireshots/:name/:country' => sub($c) {
        $payload->{data}->{full_name} = $c->stash('name');
        $payload->{data}->{country}   = $c->stash('country');
        $c->render( json => $payload );
    };
    
    post '/batchAPI' => sub ($c) {
        my $foo = parse_json $c->req->body;
        $c->render(json => $foo);
    };
}
app->start;
