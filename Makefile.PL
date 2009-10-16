use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'TextMiner',
    AUTHOR              => 'Elias Showk <elias.showk@iscpif.fr>',
    VERSION_FROM        => 'lib/TextMiner.pm',
    ABSTRACT_FROM       => 'lib/TextMiner.pm',
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'version'    => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'TextMiner-*' },
);
