param(
  [Parameter(Mandatory = $true)][string]$NodeSourceDir,
  [Parameter(Mandatory = $true)][string]$TargetTriple,
  [Parameter(Mandatory = $true)][string]$OutputRoot
)

$ErrorActionPreference = "Stop"

$ArtifactDir = Join-Path $OutputRoot $TargetTriple
$ReleaseDir = Join-Path $NodeSourceDir "out\Release"
$IncludeDir = Join-Path $NodeSourceDir "deps\v8\include"

$Libs = @(
  "v8_snapshot.lib",
  "v8_initializers.lib",
  "v8_init.lib",
  "v8_compiler.lib",
  "v8_turboshaft.lib",
  "v8_base_without_compiler.lib",
  "v8_libplatform.lib",
  "v8_libbase.lib",
  "v8_zlib.lib",
  "v8_libsampler.lib",
  "icui18n.lib",
  "icuucx.lib",
  "icudata.lib"
)

if (!(Test-Path (Join-Path $IncludeDir "v8.h"))) {
  throw "missing V8 headers at $IncludeDir"
}

if (Test-Path $ArtifactDir) {
  Remove-Item -Recurse -Force $ArtifactDir
}

New-Item -ItemType Directory -Force (Join-Path $ArtifactDir "include\v8") | Out-Null
New-Item -ItemType Directory -Force (Join-Path $ArtifactDir "lib") | Out-Null
Copy-Item -Recurse -Force (Join-Path $IncludeDir "*") (Join-Path $ArtifactDir "include\v8")

foreach ($Lib in $Libs) {
  $Source = Get-ChildItem -Path $ReleaseDir -Recurse -File -Filter $Lib | Select-Object -First 1
  if ($null -eq $Source) {
    throw "missing archive $Lib under $ReleaseDir"
  }
  $HeaderBytes = Get-Content -Path $Source.FullName -Encoding Byte -TotalCount 8
  $Header = -join ($HeaderBytes | ForEach-Object { [char]$_ })
  if ($Header -eq "!<thin>`n") {
    throw "thin archive $($Source.FullName) is not supported by the Windows package script; build a non-thin MSVC .lib or package from a normal archive output"
  }
  Copy-Item -Force $Source.FullName (Join-Path $ArtifactDir "lib\$Lib")
}

$ShaLines = foreach ($Lib in $Libs) {
  $Path = Join-Path $ArtifactDir "lib\$Lib"
  $Hash = (Get-FileHash -Algorithm SHA256 $Path).Hash.ToLowerInvariant()
  "$Hash lib/$Lib"
}

$ShaLines | Set-Content -Encoding ascii (Join-Path $ArtifactDir "SHA256SUMS")
Write-Output "wrote $ArtifactDir"
