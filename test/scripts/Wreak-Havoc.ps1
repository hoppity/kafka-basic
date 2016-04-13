param(
    [parameter(Mandatory=$true)]
    $Zookeeper,
    [parameter(Mandatory=$true)]
    $Topic,
    [parameter(Mandatory=$true)]
    $Group,
    $NumberOfMessages = 10000,
    $WaitBeforeKill = 60,
    $NumberOfConsumers = 3,
    $ThreadsPerConsumer = 2
)

$testDirectory = Split-Path -Parent $PSScriptRoot

$start = [datetime]::Now

rm .\*rcv.txt

$consumerArgs = @( "chaos", "-z", $Zookeeper, "-g", $Group, "-t", $Topic, "-h", $ThreadsPerConsumer, "-b", "100")

$consumers = @()

Write-Host "Starting $NumberOfConsumers consumers"

for ($i = 0; $i -lt $NumberOfConsumers; $i++) {
    $process = Start-Process `
        ( Join-Path $testDirectory "Test.Consumer\bin\Debug\Consumer.exe" ) `
        -ArgumentList $consumerArgs `
        -NoNewWindow `
        -RedirectStandardOutput ".\$($i)rcv.txt" `
        -PassThru

    $consumers += $process

    sleep 2
}

Write-Host "Starting producer to publish $NumberOfMessages messages"

$producerArgs = @( "-z", $Zookeeper, "-t", $Topic, "-b", "10", "-m", "$NumberOfMessages", "-s", "100")

& ( Join-Path $testDirectory "Test.Producer\bin\Debug\Producer.exe" ) $producerArgs > producer.txt

Write-Host "Finished publishing. Waiting $WaitBeforeKill seconds before terminating consumers."

sleep -Seconds $WaitBeforeKill

$consumers | %{ if (-not $_.HasExited) { $_.Kill() } }

Write-Host "Inspecting output of consumers to determine how many messages were recieved."

$messages = `
    Get-ChildItem -Filter *rcv.txt | `
    Get-Content | `
    %{ 
        if ( $_ -match 'P:(\d+),O:(\d+).*' ) {
            New-Object -TypeName psobject -Property @{
                Partition = $Matches[1]
                Offset = $Matches[2]
            }
        }
    }

$end = [datetime]::Now
$time = ( $end - $start ).ToString('c')

Write-Host "Total received : $( $messages.Length )"

$uniqueMessages = $( ($messages | Sort-Object Partition, Offset -Unique).Length )
Write-Host "Total unique   : $uniqueMessages"

Write-Host "Total time     : $time"

if ( $uniqueMessages -ne $NumberOfMessages ) {
    Write-Error "Expected to receive $NumberOfMessages unique messages, but only received $uniqueMessages."
}
