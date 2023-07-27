# Dafda.Avro

**TL;DR:** `Dafda.Avro` is an extension of Dafda, that support the use of Avro schemas.

## Building and Releasing

Dafda.Avro is like Dafda build and released using a combination of `make` and [GitHub Actions](https://github.com/dfds/dafda.avro/blob/master/.github/workflows/release.yml)

You will need the dotnet sdk. Refer to the [Microsoft Documentation](https://docs.microsoft.com/en-us/dotnet/core/install/linux-ubuntu) on how to install

Dafda is available on [NuGet](https://www.nuget.org/packages/Dafda.Avro/).

### Versioning
Run:

```bash
make version
```

And input the new version of Dafda. This will update the `Dafda.csproj` with the new version.

### NuGet Packages

Run:

```bash
make release
git push --follow-tags
```

Will git tag with the current version (see [Versioning](#versioning), and GitHub Actions will take care of building, and pushing to NuGet.
