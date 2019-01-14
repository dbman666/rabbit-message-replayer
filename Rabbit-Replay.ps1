function Find-RabbitMessages() {
    [CmdletBinding()]
    Param (
        [string] $Folder = '.',
        [ValidateSet('*.rdq', '*.idx')]
        [string[]] $Patterns = @("*.rdq", "*.idx"),
        [string] $Match,
        [int] $Threads = 0,
        [string] $OutQueues,
        [string] $OutTypes,
        [string] $OutFiles
    )

    $command = "rabbit-message-replayer --output Json --folder $Folder --pattern '{0}' --threads=$Threads --match=`"$Match`"" -f ($Patterns -join ';')

    Write-Verbose $command

    $result = (Invoke-Expression $command | ConvertFrom-Json).SyncRoot
    $result = @{
        Files = $result.Files | Sort-Object -Descending Count | Add-Member Type Files -PassThru
        FileTypes = $result.FileTypes | Sort-Object -Descending Count | Add-Member Type FileTypes -PassThru
        Queues = $result.Queues | Sort-Object -Descending Count | Add-Member Type Queues -PassThru
        QueueTypes = $result.QueueTypes | Sort-Object -Descending Count | Add-Member Type QueueTypes -PassThru
    }

    $result.Files | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    $result.FileTypes | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    $result.Queues | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    $result.QueueTypes | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    Update-TypeData -TypeName Rabbit.Stat -DefaultDisplayPropertySet Name, Messages, Size, Average, Minimum, Maximum -Force

    if ($OutQueues) { Set-Variable -scope 1 -Name $OutQueues -Value $result.Queues  }
    if ($OutTypes) { Set-Variable -scope 1 -Name $OutTypes -Value $result.Types }
    if ($OutFiles) { Set-Variable -scope 1 -Name $OutFiles -Value $result.Files }
    if ($OutFileTypes) { Set-Variable -scope 1 -Name $OutFiles -Value $result.Files }
    Set-Variable -scope 1 -Name RabbitMessages -Value $result

    $result.Files + $result.Queues + $result.QueueTypes + $result.FileTypes
}