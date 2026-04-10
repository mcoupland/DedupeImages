using System.Globalization;
using System.Security.Cryptography;

namespace ImageDedupe;
public static class ImageDeduper
{
    public static void DedupeImages(
        string sourceRoot,
        string canonicalDir,
        string hashMatchDir,
        string metaMatchDir,
        bool recursive = true)
    {
        if (string.IsNullOrWhiteSpace(sourceRoot)) throw new ArgumentException(nameof(sourceRoot));
        Directory.CreateDirectory(canonicalDir);
        Directory.CreateDirectory(hashMatchDir);
        Directory.CreateDirectory(metaMatchDir);

        var searchOpt = recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

        // Adjust extensions as needed
        var exts = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            ".jpg",".jpeg",".png",".gif",".bmp",".tif",".tiff",".webp",".heic"
        };

        // 1) Gather candidates
        var files = Directory.EnumerateFiles(sourceRoot, "*.*", searchOpt)
            .Where(f => exts.Contains(Path.GetExtension(f)))
            // Avoid re-processing files already in destination folders if they are under sourceRoot
            .Where(f => !IsUnder(f, canonicalDir) && !IsUnder(f, hashMatchDir) && !IsUnder(f, metaMatchDir))
            .ToList();

        // 2) Build metadata for each file (hash + size + dimensions)
        var infos = new List<ImageInfo>(files.Count);
        foreach (var file in files)
        {
            try
            {
                var fi = new FileInfo(file);

                // Hash first (your requirement)
                var hash = ComputeSha256Hex(file);

                // Dimensions (pixels)
                var (w, h) = TryGetDimensions(file);

                infos.Add(new ImageInfo(
                    FullPath: file,
                    FileName: Path.GetFileName(file),
                    SizeBytes: fi.Length,
                    Sha256Hex: hash,
                    Width: w,
                    Height: h,
                    CreationUtc: fi.CreationTimeUtc,
                    LastWriteUtc: fi.LastWriteTimeUtc
                ));
            }
            catch
            {
                // If something is unreadable/corrupt, skip it (or log it, depending on your needs)
            }
        }

        // We'll mark moved files so meta pass only considers remaining.
        var moved = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // 3) Pass A: Deduplicate by hash
        foreach (var g in infos.GroupBy(i => i.Sha256Hex, StringComparer.OrdinalIgnoreCase)
                               .Where(g => g.Count() > 1))
        {
            var group = g.Where(i => !moved.Contains(i.FullPath)).ToList();
            if (group.Count <= 1) continue;

            var canonical = PickCanonical(group);

            // Move canonical to canonical folder
            MoveToFolder(canonical.FullPath, canonicalDir);
            moved.Add(canonical.FullPath);

            // Move the rest to hash-match folder
            foreach (var dupe in group.Where(x => !PathEquals(x.FullPath, canonical.FullPath)))
            {
                MoveToFolder(dupe.FullPath, hashMatchDir);
                moved.Add(dupe.FullPath);
            }
        }

        // 4) Pass B: Deduplicate by (size + dimensions) among remaining files
        //    NOTE: This can produce false positives (same size and dimensions but different content).
        var remaining = infos.Where(i => !moved.Contains(i.FullPath)).ToList();

        foreach (var g in remaining
            .Where(i => i.Width.HasValue && i.Height.HasValue) // only those with dimensions
            .GroupBy(i => new MetaKey(i.SizeBytes, i.Width!.Value, i.Height!.Value))
            .Where(g => g.Count() > 1))
        {
            var group = g.Where(i => !moved.Contains(i.FullPath)).ToList();
            if (group.Count <= 1) continue;

            var canonical = PickCanonical(group);

            MoveToFolder(canonical.FullPath, canonicalDir);
            moved.Add(canonical.FullPath);

            foreach (var dupe in group.Where(x => !PathEquals(x.FullPath, canonical.FullPath)))
            {
                MoveToFolder(dupe.FullPath, metaMatchDir);
                moved.Add(dupe.FullPath);
            }
        }
    }

    // --- Helpers ---

    private static ImageInfo PickCanonical(List<ImageInfo> group)
    {
        // Canonical selection policy (tweak as you like):
        // Prefer: earliest creation time, then earliest last write, then shortest path, then name.
        return group.OrderBy(i => i.CreationUtc)
                    .ThenBy(i => i.LastWriteUtc)
                    .ThenBy(i => i.FullPath.Length)
                    .ThenBy(i => i.FileName, StringComparer.OrdinalIgnoreCase)
                    .First();
    }

    private static string ComputeSha256Hex(string filePath)
    {
        using var sha = SHA256.Create();
        using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read,
                                          bufferSize: 1024 * 1024, options: FileOptions.SequentialScan);
        var hash = sha.ComputeHash(stream);
        return Convert.ToHexString(hash); // .NET 5+
    }

    private static (int? w, int? h) TryGetDimensions(string filePath)
    {
        try
        {
            // System.Drawing is best on Windows. For cross-platform, consider ImageSharp.
            using var img = System.Drawing.Image.FromFile(filePath);
            return (img.Width, img.Height);
        }
        catch
        {
            return (null, null);
        }
    }

    private static void MoveToFolder(string srcPath, string destFolder)
    {
        Directory.CreateDirectory(destFolder);
        var destPath = EnsureUniquePath(Path.Combine(destFolder, Path.GetFileName(srcPath)));

        // File.Move in .NET 6 has overload with overwrite; using safe pattern for broad compatibility
        File.Move(srcPath, destPath);
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

    private static bool IsUnder(string path, string folder)
    {
        var p = Path.GetFullPath(path).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var f = Path.GetFullPath(folder).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return p.StartsWith(f, StringComparison.OrdinalIgnoreCase);
    }

    private static bool PathEquals(string a, string b) =>
        string.Equals(Path.GetFullPath(a), Path.GetFullPath(b), StringComparison.OrdinalIgnoreCase);

    private readonly record struct MetaKey(long SizeBytes, int Width, int Height);

    private sealed record ImageInfo(
        string FullPath,
        string FileName,
        long SizeBytes,
        string Sha256Hex,
        int? Width,
        int? Height,
        DateTime CreationUtc,
        DateTime LastWriteUtc);
}
