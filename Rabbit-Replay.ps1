function Find-RabbitMessages() {
    [CmdletBinding()]
    Param (
        [string] $Folder = '.',
        [ValidateSet('*.rdq', '*.idx')]
        [string[]] $Patterns = @("*.rdq", "*.idx"),
        [int] $Threads = 0,
        [string] $OutQueues,
        [string] $OutTypes,
        [string] $OutFiles
    )

    $command = "rabbit-message-replayer --output Json --folder $Folder --pattern {0} --threads=$Threads" -f ($Patterns -join ';')
    $result = (Invoke-Expression $command | ConvertFrom-Json).SyncRoot
    $result = @{
        Files = $result.Files | Sort-Object -Descending Count
        Queues = $result.Queues | Sort-Object -Descending Count
        Types = $result.Files | Sort-Object -Descending Count
    }

    $result.Queues | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    $result.Types | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    $result.Files | ForEach-Object { $_.PSObject.TypeNames.Insert(0, "Rabbit.Stat") }
    Update-TypeData -TypeName Rabbit.Stat -DefaultDisplayPropertySet Name, Count, Size, Average -Force

    if ($OutQueues) { Set-Variable -scope 1 -Name $OutQueues -Value $result.Queues }
    if ($OutTypes) { Set-Variable -scope 1 -Name $OutTypes -Value $result.Types }
    if ($OutFiles) { Set-Variable -scope 1 -Name $OutFiles -Value $result.Files }
    Set-Variable -scope 1 -Name RabbitMessages -Value $result
    $result.Queues
}