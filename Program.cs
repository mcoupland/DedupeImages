// See https://aka.ms/new-console-template for more information
var sourceRoot= @"G:\Duplicates";
var canonicalDir= @"G:\CanonicalFiles";
var hashMatchDir = @"G:\HashMatches";
var metaMatchDir = @"G:\MetaMatches";

await ImageDedupeFastest.DedupeAsync(
    sourceRoot,
    canonicalDir,
    hashMatchDir,
    metaMatchDir,
    options: new ImageDedupeFastest.Options
    {
        DegreeOfParallelism = 3,   // good for HDD
        EnableMetaMatch = true,
        Verbose = true
    });
