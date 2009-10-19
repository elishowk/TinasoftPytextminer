#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Exception;
use YAML::Syck;
use DateTime;
use DateTime::Format::Flexible;
use KiokuDB::Util qw(set);

use ok 'TextMiner::Corpus';
use ok 'TextMiner::Document';
use ok 'TextMiner::Target';
use ok 'TextMiner::Ngram';
use ok 'KiokuDB';
use ok 'KiokuDB::Backend::Hash';

my $dir = KiokuDB->new(
    backend => KiokuDB::Backend::Hash->new,
);
my $scope = $dir->new_scope;

my $contents = LoadFile( 't/data/test-env.yaml' );
my $corpus = TextMiner::Corpus->new( name => $contents->{ corpus } );
my $corpus_id = $dir->store($corpus);
my @documents;

foreach ( @{ $contents->{ documents } } ) {
	my $document = TextMiner::Document->new(
		corpus => $corpus,
		title => $_->{ title },
		timestamp => DateTime::Format::Flexible->parse_datetime($_->{ date }),
		content => Dump($_),
	);
	push @documents, $document;
	my $document_id = $dir->store($document);
	warn "Stored document id = $document_id";
	my $target = TextMiner::Target->new(
		#document => $document,
		type => 'content',
		sanitizedTarget => $_->{ 'content' },
		minSize => 3,
		maxSize => 3,
	);
	my @targets = ( $target );
	$document->targets( set(@targets) );
	my $target_id = $dir->store($target);
	warn "Stored target id = $target_id";
}
$corpus->documents( set(@documents) );

#$scope = $dir->new_scope;
map {
	map {
		$_->extract_ngrams();
		map {
			warn Dump $_->ngram, "where length = ".$_->length;
		} $_->ngrams->members;
	} $_->targets->members;
} $corpus->documents->members;

done_testing;
