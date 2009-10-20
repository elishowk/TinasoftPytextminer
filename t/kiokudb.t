#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Exception;
use YAML::Syck;
use DateTime;
use DateTime::Format::Flexible;
use KiokuDB::Util qw(set);
use Encode;
use Test::utf8;
use encoding::warnings;

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
#$YAML::Syck::ImplicitUnicode = 1;
my $contents = LoadFile( 't/data/test-env.yaml' );
my $corpus = TextMiner::Corpus->new( name => $contents->{ corpus } );
my $corpus_id = $dir->store($corpus);
my @documents;
my @target_ids;

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
		type => 'content',
		target => $_->{ 'content' },
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
my @new_target_ids;
map {
	map {
		$_->extract_ngrams();
		map {
			my @ng = keys %{$_->ngram};
			warn $ng[0];
			#unless ( Encode::is_utf8( $ng[0] ) ) {
			#is_valid_string($ng[0]);
			#is_sane_utf8($ng[0]);
			#is_flagged_utf8($ng[0]);
			#is_within_latin_1($ng[0]);
			#}
			my $new_ngram_id = $dir->store($_);
		} $_->ngrams->members;
		my $new_target_id = $dir->store($_);
	} $_->targets->members;
	my $new_doc_id = $dir->store($_);
} $corpus->documents->members;

done_testing;
