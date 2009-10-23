use Test::More tests => 5;

BEGIN {
use_ok( 'TextMiner' );
use_ok( 'TextMiner::Corpus' );
use_ok( 'TextMiner::Document' );
use_ok( 'TextMiner::Target' );
use_ok( 'TextMiner::Ngram' );
}

diag( "Testing TextMiner $TextMiner::VERSION" );
