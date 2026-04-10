using System.Buffers;
using System.Collections.Concurrent;
using System.Drawing; // System.Drawing.Common (Windows)
using System.Globalization;
using System.Security.Cryptography;

/// <summary>
/// Fast image deduper optimized for HDD workloads and SAME-VOLUME moves.
///
/// REQUIRED BEHAVIOR (per your last message):
/// - When finished: there are NO image files left in sourceRoot (everything is moved).
/// - Any image with no duplicates is treated as canonical and moved to canonicalDir.
///
/// Pipeline:
///  1) Enumerate image files
///  2) Group by file size (cheap)
///  3) For size-collision groups: compute QUICK hash (first+last chunk + length)
///  4) For quick-hash collisions: compute FULL SHA-256 => true dupes -> (canonical + hash-match)
///  5) Optional: META match among remaining by (size + dimensions) -> (canonical + meta-match)
///  6) Move ALL remaining (non-duplicate) images to canonicalDir
///
/// Notes:
/// - On the same volume, File.Move is typically metadata-only (rename), very fast.
/// - Hashing dominates runtime; this pipeline minimizes full hashing.
/// - Meta-match can produce false positives; enable only if you really want it.
/// </summary>
public static class ImageDedupeFastest
{
    public static int smartMoved = 0;

    public sealed class Options
    {
        public bool Recursive { get; init; } = true;

        /// <summary>
        /// Meta-match: group remaining files by (size + width + height) and treat as duplicates.
        /// WARNING: can create false positives.
        /// </summary>
        public bool EnableMetaMatch { get; init; } = true;

        /// <summary>
        /// Quick hash reads first+last chunk (and length). 1MB is a good balance.
        /// </summary>
        public int QuickHashChunkBytes { get; init; } = 1_048_576;

        /// <summary>
        /// HDDs hate high parallel random reads; keep low (2-4).
        /// </summary>
        public int DegreeOfParallelism { get; init; } =
            Math.Max(2, Math.Min(Environment.ProcessorCount, 4));

        /// <summary>
        /// Buffer used for full SHA-256 hashing.
        /// </summary>
        public int FullHashBufferBytes { get; init; } = 1_048_576;

        public HashSet<string> Extensions { get; init; } = new(StringComparer.OrdinalIgnoreCase)
        {
            ".jpg",".jpeg",".png",".gif",".bmp",".tif",".tiff",".webp",".heic"
        };

        /// <summary>
        /// Dimension reader. Default uses System.Drawing (Windows).
        /// Replace for cross-platform (e.g., ImageSharp).
        /// </summary>
        public IImageDimensionReader DimensionReader { get; init; } = new SystemDrawingDimensionReader();

        /// <summary>
        /// Skip files already under any destination directory (if destinations are inside sourceRoot).
        /// </summary>
        public bool SkipDestinationFoldersIfInsideSource { get; init; } = true;

        /// <summary>
        /// Optional: log basic progress to Console.
        /// </summary>
        public bool Verbose { get; init; } = false;
    }

    public interface IImageDimensionReader
    {
        (int? width, int? height) TryGetDimensions(string path);
    }

    public sealed class SystemDrawingDimensionReader : IImageDimensionReader
    {
        public (int? width, int? height) TryGetDimensions(string path)
        {
            try
            {
                using var img = Image.FromFile(path);
                return (img.Width, img.Height);
            }
            catch { return (null, null); }
        }
    }

    private sealed record FileRec(
        string Path,
        string Name,
        long SizeBytes,
        DateTime CreationUtc,
        DateTime LastWriteUtc);

    private readonly record struct MetaKey(long SizeBytes, int Width, int Height);

    /// <summary>
    /// Main entry point. Moves ALL images out of sourceRoot into canonical/hash/meta buckets.
    /// </summary>
    public static async Task DedupeAsync(
        string sourceRoot,
        string canonicalDir,
        string hashMatchDir,
        string metaMatchDir,
        Options? options = null,
        CancellationToken ct = default)
    {
        int smartMoved = 0;
        options ??= new Options();

        if (string.IsNullOrWhiteSpace(sourceRoot)) throw new ArgumentException(nameof(sourceRoot));
        Directory.CreateDirectory(canonicalDir);
        Directory.CreateDirectory(hashMatchDir);
        Directory.CreateDirectory(metaMatchDir);

        var searchOpt = options.Recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

        // ---- 1) Enumerate ----
        var allFiles = Directory.EnumerateFiles(sourceRoot, "*.*", searchOpt)
            .Where(f => options.Extensions.Contains(Path.GetExtension(f)))
            .ToList();

        // Optionally avoid re-processing files that already live in output folders.
        if (options.SkipDestinationFoldersIfInsideSource)
        {
            allFiles = allFiles
                .Where(f => !IsUnder(f, canonicalDir))
                .Where(f => !IsUnder(f, hashMatchDir))
                .Where(f => !IsUnder(f, metaMatchDir))
                .ToList();
        }

        // Minimal metadata upfront
        var infos = new List<FileRec>(allFiles.Count);
        foreach (var p in allFiles)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var fi = new FileInfo(p);
                infos.Add(new FileRec(
                    Path: p,
                    Name: Path.GetFileName(p),
                    SizeBytes: fi.Length,
                    CreationUtc: fi.CreationTimeUtc,
                    LastWriteUtc: fi.LastWriteTimeUtc));
            }
            catch { /* skip */ }
        }

        // Fast lookup for canonical selection
        var infoByPath = infos.ToDictionary(i => Path.GetFullPath(i.Path), i => i, StringComparer.OrdinalIgnoreCase);

        if (options.Verbose)
            Console.WriteLine($"Found {infos.Count:n0} image files.");

        // Track moved to avoid double-processing
        var moved = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        // ---- 2) Group by size ----
        var sizeGroups = infos.GroupBy(x => x.SizeBytes)
                              .Where(g => g.Count() > 1)
                              .ToList();

        if (options.Verbose)
            Console.WriteLine($"Size-collision groups: {sizeGroups.Count:n0}");

        // ---- 3) Quick hash for size collisions ----
        var quickHashByPath = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        int quickHashProcessed = 0;

        await Parallel.ForEachAsync(sizeGroups, new ParallelOptions
        {
            MaxDegreeOfParallelism = options.DegreeOfParallelism,
            CancellationToken = ct
        }, async (group, token) =>
        {
            foreach (var f in group)
            {
                token.ThrowIfCancellationRequested();
                if (moved.ContainsKey(f.Path)) continue;
                if (quickHashByPath.ContainsKey(f.Path)) continue;

                try
                {
                    var qh = QuickHashFirstLast(f.Path, options.QuickHashChunkBytes);
                    quickHashByPath.TryAdd(f.Path, qh);

                    int n = Interlocked.Increment(ref quickHashProcessed);

                    // Optional progress logging every 1000 files
                    if ((n % 100) == 0)
                        Console.WriteLine($"Quick hashed {n:n0} files...");
                }
                catch { /* skip */ }
            }

            await ValueTask.CompletedTask;
        });

        // ---- 4) For quick-hash collisions, compute full SHA-256 and dedupe ----
        var fullHashByPath = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        int fullHashProcessed = 0;
        foreach (var sizeGroup in sizeGroups)
        {
            ct.ThrowIfCancellationRequested();

            var members = sizeGroup.Select(x => x.Path)
                                   .Where(p => quickHashByPath.ContainsKey(p))
                                   .ToList();
            if (members.Count <= 1) continue;

            var qGroups = members.GroupBy(p => quickHashByPath[p], StringComparer.OrdinalIgnoreCase)
                                 .Where(g => g.Count() > 1);

            foreach (var qg in qGroups)
            {
                var paths = qg.Where(p => !moved.ContainsKey(p)).ToList();
                if (paths.Count <= 1) continue;

                // Full hash only these candidates
                await Parallel.ForEachAsync(paths, new ParallelOptions
                {
                    MaxDegreeOfParallelism = options.DegreeOfParallelism,
                    CancellationToken = ct
                }, async (path, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    if (moved.ContainsKey(path)) return;
                    if (fullHashByPath.ContainsKey(path)) return;

                    try
                    {
                        var fh = ComputeSha256Hex(path, options.FullHashBufferBytes);
                        fullHashByPath.TryAdd(path, fh);

                        int n = Interlocked.Increment(ref fullHashProcessed);

                        // Optional progress logging every 1000 files
                        if ((n % 100) == 0)
                            Console.WriteLine($"Full hashed {n:n0} files...");
                    }
                    catch { /* skip */ }

                    await ValueTask.CompletedTask;
                });

                // Group by full hash => true duplicates
                var hashGroups = paths
                    .Where(p => fullHashByPath.ContainsKey(p))
                    .GroupBy(p => fullHashByPath[p], StringComparer.OrdinalIgnoreCase)
                    .Where(g => g.Count() > 1);

                foreach (var hg in hashGroups)
                {
                    var dupPaths = hg.Where(p => !moved.ContainsKey(p)).ToList();
                    if (dupPaths.Count <= 1) continue;

                    var canonical = PickCanonical(infoByPath, dupPaths);

                    MoveSmart(canonical, canonicalDir);
                    moved.TryAdd(canonical, 1);

                    foreach (var dp in dupPaths.Where(p => !PathEquals(p, canonical)))
                    {
                        MoveSmart(dp, hashMatchDir);
                        moved.TryAdd(dp, 1);
                    }
                }
            }
        }

        int metaMatchProcessed = 0;
        // ---- 5) Meta-match among remaining: (size + dimensions) ----
        if (options.EnableMetaMatch)
        {
            var remaining = infos.Where(i => !moved.ContainsKey(i.Path)).ToList();
            if (options.Verbose)
                Console.WriteLine($"Remaining after hash dedupe: {remaining.Count:n0}");

            var remainingSizeGroups = remaining.GroupBy(i => i.SizeBytes)
                                               .Where(g => g.Count() > 1)
                                               .ToList();

            var dimsByPath = new ConcurrentDictionary<string, (int? w, int? h)>(StringComparer.OrdinalIgnoreCase);

            await Parallel.ForEachAsync(remainingSizeGroups, new ParallelOptions
            {
                MaxDegreeOfParallelism = options.DegreeOfParallelism,
                CancellationToken = ct
            }, async (group, token) =>
            {
                foreach (var f in group)
                {
                    token.ThrowIfCancellationRequested();
                    if (moved.ContainsKey(f.Path)) continue;
                    if (dimsByPath.ContainsKey(f.Path)) continue;

                    try
                    {
                        dimsByPath.TryAdd(f.Path, options.DimensionReader.TryGetDimensions(f.Path));
                    }
                    catch
                    {
                        dimsByPath.TryAdd(f.Path, (null, null));
                    }
                    int n = Interlocked.Increment(ref metaMatchProcessed);

                    // Optional progress logging every 1000 files
                    if ((n % 100) == 0)
                        Console.WriteLine($"Meta matched {n:n0} files...");
                }

                await ValueTask.CompletedTask;
            });

            var metaGroups = remaining
                .Where(r => !moved.ContainsKey(r.Path))
                .Select(r =>
                {
                    var (w, h) = dimsByPath.TryGetValue(r.Path, out var d) ? d : (null, null);
                    return (rec: r, w, h);
                })
                .Where(x => x.w.HasValue && x.h.HasValue)
                .GroupBy(x => new MetaKey(x.rec.SizeBytes, x.w!.Value, x.h!.Value))
                .Where(g => g.Count() > 1);

            int metaSets = 0;

            foreach (var mg in metaGroups)
            {
                ct.ThrowIfCancellationRequested();

                var dupPaths = mg.Select(x => x.rec.Path)
                                 .Where(p => !moved.ContainsKey(p))
                                 .ToList();
                if (dupPaths.Count <= 1) continue;

                metaSets++;

                var canonical = PickCanonical(infoByPath, dupPaths);

                MoveSmart(canonical, canonicalDir);
                moved.TryAdd(canonical, 1);

                foreach (var dp in dupPaths.Where(p => !PathEquals(p, canonical)))
                {
                    MoveSmart(dp, metaMatchDir);
                    moved.TryAdd(dp, 1);
                }
            }

            if (options.Verbose)
                Console.WriteLine($"Meta-match sets moved: {metaSets:n0}");
        }

        // ---- 6) Move ALL remaining images to canonical (no dupes) ----
        // This satisfies: "no files left in the source directory"
        var stillHere = infos.Where(i => !moved.ContainsKey(i.Path)).Select(i => i.Path).ToList();

        if (options.Verbose)
            Console.WriteLine($"Non-duplicates to move to canonical: {stillHere.Count:n0}");

        await Parallel.ForEachAsync(stillHere, new ParallelOptions
        {
            MaxDegreeOfParallelism = options.DegreeOfParallelism,
            CancellationToken = ct
        }, async (path, token) =>
        {
            token.ThrowIfCancellationRequested();
            if (moved.ContainsKey(path)) return;

            try
            {
                MoveSmart(path, canonicalDir);
                moved.TryAdd(path, 1);
            }
            catch
            {
                // If a move fails, you can choose to throw instead.
                // For safety, we just leave it unmoved.
            }

            await ValueTask.CompletedTask;
        });

        if (options.Verbose)
        {
            Console.WriteLine($"Done. Moved {moved.Count:n0} files.");
            Console.WriteLine($"Canonical: {canonicalDir}");
            Console.WriteLine($"Hash matches: {hashMatchDir}");
            Console.WriteLine($"Meta matches: {metaMatchDir}");
        }
    }

    // ----------------- FAST HASHING -----------------

    /// <summary>
    /// Quick hash: SHA-256 over (file length + first chunk + last chunk).
    /// Reads at most ~2 * chunkBytes from disk (plus a few bytes).
    /// </summary>
    public static string QuickHashFirstLast(string path, int chunkBytes)
    {
        using var sha = SHA256.Create();

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 1024 * 1024, options: FileOptions.RandomAccess);

        long len = fs.Length;
        var lenBytes = BitConverter.GetBytes(len);

        sha.TransformBlock(lenBytes, 0, lenBytes.Length, null, 0);

        // first
        var first = ReadChunk(fs, 0, (int)Math.Min(len, chunkBytes));
        sha.TransformBlock(first, 0, first.Length, null, 0);

        // last
        if (len > chunkBytes)
        {
            int lastSize = (int)Math.Min(len, chunkBytes);
            long lastPos = Math.Max(0, len - lastSize);
            var last = ReadChunk(fs, lastPos, lastSize);
            sha.TransformFinalBlock(last, 0, last.Length);
        }
        else
        {
            sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        }

        return Convert.ToHexString(sha.Hash!);
    }

    private static byte[] ReadChunk(FileStream fs, long position, int size)
    {
        fs.Position = position;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(Math.Max(size, 1));
        try
        {
            int read = 0;
            while (read < size)
            {
                int n = fs.Read(buffer, read, size - read);
                if (n == 0) break;
                read += n;
            }

            var result = new byte[read];
            Buffer.BlockCopy(buffer, 0, result, 0, read);
            return result;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Full SHA-256 of entire file. Uses a large buffer and sequential scan hint.
    /// </summary>
    public static string ComputeSha256Hex(string path, int bufferBytes)
    {
        using var sha = SHA256.Create();
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: bufferBytes, options: FileOptions.SequentialScan);

        var hash = sha.ComputeHash(fs);
        return Convert.ToHexString(hash);
    }

    // ----------------- CANONICAL PICK & MOVE -----------------

    private static string PickCanonical(Dictionary<string, FileRec> infoByPath, List<string> paths)
    {
        // Stable canonical policy:
        // Prefer earliest creation time, then earliest last-write, then shorter path, then name.
        FileRec? best = null;

        foreach (var p in paths)
        {
            var fp = Path.GetFullPath(p);
            if (!infoByPath.TryGetValue(fp, out var rec)) continue;

            if (best is null)
            {
                best = rec;
                continue;
            }

            // Compare
            int c = rec.CreationUtc.CompareTo(best.CreationUtc);
            if (c < 0) { best = rec; continue; }
            if (c > 0) continue;

            c = rec.LastWriteUtc.CompareTo(best.LastWriteUtc);
            if (c < 0) { best = rec; continue; }
            if (c > 0) continue;

            c = rec.Path.Length.CompareTo(best.Path.Length);
            if (c < 0) { best = rec; continue; }
            if (c > 0) continue;

            if (string.Compare(rec.Name, best.Name, StringComparison.OrdinalIgnoreCase) < 0)
                best = rec;
        }

        return best?.Path ?? paths[0];
    }

    /// <summary>
    /// Fast move on same volume (rename). If different volume, copy+delete.
    /// Ensures unique destination name.
    /// </summary>
    public static void MoveSmart(string srcPath, string destFolder)
    {        
        smartMoved++;
        if(smartMoved % 100 == 0)
            Console.WriteLine($"Smart moved {smartMoved:n0} files...");

        Directory.CreateDirectory(destFolder);

        var destPath = EnsureUniquePath(Path.Combine(destFolder, Path.GetFileName(srcPath)));

        var srcRoot = Path.GetPathRoot(Path.GetFullPath(srcPath))!;
        var dstRoot = Path.GetPathRoot(Path.GetFullPath(destPath))!;

        if (string.Equals(srcRoot, dstRoot, StringComparison.OrdinalIgnoreCase))
        {
            File.Move(srcPath, destPath);
            return;
        }

        File.Copy(srcPath, destPath);
        File.Delete(srcPath);
    }

    private static string EnsureUniquePath(string desiredPath)
    {
        if (!File.Exists(desiredPath)) return desiredPath;

        var dir = Path.GetDirectoryName(desiredPath)!;
        var name = Path.GetFileNameWithoutExtension(desiredPath);
        var ext = Path.GetExtension(desiredPath);

        for (int i = 1; i < int.MaxValue; i++)
        {
            var candidate = Path.Combine(dir, $"{name} ({i.ToString(CultureInfo.InvariantCulture)}){ext}");
            if (!File.Exists(candidate)) return candidate;
        }

        throw new IOException("Unable to find a unique destination filename.");
    }

    // ----------------- PATH HELPERS -----------------

    private static bool IsUnder(string path, string folder)
    {
        var p = Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var f = Path.GetFullPath(folder).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return p.StartsWith(f, StringComparison.OrdinalIgnoreCase);
    }

    private static bool PathEquals(string a, string b) =>
        string.Equals(Path.GetFullPath(a), Path.GetFullPath(b), StringComparison.OrdinalIgnoreCase);
}
