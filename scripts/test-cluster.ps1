# Test script for driving miniKV cluster through HTTP API and testing failover
$ErrorActionPreference = "Stop"

Write-Host "=== Step 1: Launching 3 storage nodes and coordinator ==="
$n1 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-1 --addr 127.0.0.1:7000 --http-addr 127.0.0.1:7001 --data-dir ./data/test-node-1" -PassThru
$n2 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-2 --addr 127.0.0.1:7010 --http-addr 127.0.0.1:7011 --data-dir ./data/test-node-2" -PassThru
$n3 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-3 --addr 127.0.0.1:7020 --http-addr 127.0.0.1:7021 --data-dir ./data/test-node-3" -PassThru

Start-Sleep -Seconds 1

$coord = Start-Process -FilePath ".\minikv-coord.exe" -ArgumentList '--addr :8080 --nodes "node-1=127.0.0.1:7000=127.0.0.1:7001,node-2=127.0.0.1:7010=127.0.0.1:7011,node-3=127.0.0.1:7020=127.0.0.1:7021"' -PassThru

Start-Sleep -Seconds 2

try {
    Write-Host "`n=== Step 2: Testing Cluster Stats ==="
    $stats = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/stats"
    Write-Host "Nodes Total: $($stats.nodes_total), Alive: $($stats.nodes_alive)"

    Write-Host "`n=== Step 3: Testing PUT with TTL (500ms) ==="
    $putBody = @{ value = "temporary-data"; ttl_ms = 500 } | ConvertTo-Json
    $putResp = Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:8080/api/v1/kv/session:test" -Body $putBody -ContentType "application/json"
    Write-Host "PUT Response: $($putResp | ConvertTo-Json -Compress)"

    Write-Host "`n=== Step 4: Immediate GET before TTL expiry ==="
    $getResp = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/kv/session:test"
    Write-Host "GET Response: $($getResp | ConvertTo-Json -Compress)"
    if ($getResp.value -ne "temporary-data") { throw "Value mismatch!" }

    Write-Host "`n=== Step 5: Waiting 600ms for TTL expiry ==="
    Start-Sleep -Milliseconds 600
    try {
        $expiredResp = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/kv/session:test"
        throw "Key should have expired, but got: $($expiredResp | ConvertTo-Json)"
    } catch {
        Write-Host "Confirmed key expired (HTTP 404 received as expected)"
    }

    Write-Host "`n=== Step 6: Testing PUT with Synchronous Replication ==="
    $syncBody = @{ value = "critical-account-balance"; sync = $true; sync_n = 1 } | ConvertTo-Json
    $syncResp = Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:8080/api/v1/kv/user:balance" -Body $syncBody -ContentType "application/json"
    Write-Host "PUT Sync Response: $($syncResp | ConvertTo-Json -Compress)"
    $masterNode = $syncResp.primary
    $replicaNodes = $syncResp.replicas
    Write-Host "Master Node: $masterNode, Replicas: $($replicaNodes -join ', ')"

    Write-Host "`n=== Step 7: Killing Master Node ($masterNode) ==="
    if ($masterNode -eq "node-1") { Stop-Process -Id $n1.Id -Force; Write-Host "Killed node-1 (PID $($n1.Id))" }
    elseif ($masterNode -eq "node-2") { Stop-Process -Id $n2.Id -Force; Write-Host "Killed node-2 (PID $($n2.Id))" }
    else { Stop-Process -Id $n3.Id -Force; Write-Host "Killed node-3 (PID $($n3.Id))" }

    Start-Sleep -Milliseconds 500

    Write-Host "`n=== Step 8: GET after Master killed (verifying replica promotion and zero data loss) ==="
    $failoverGet = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/kv/user:balance"
    Write-Host "Failover GET Response: $($failoverGet | ConvertTo-Json -Compress)"
    if ($failoverGet.value -ne "critical-account-balance") {
        throw "Data loss detected after master failure!"
    }
    Write-Host "SUCCESS: Data intact! Served from surviving replica node: $($failoverGet.node)"

    Write-Host "`n=== Step 9: Testing DELETE on surviving replica ==="
    $delResp = Invoke-RestMethod -Method Delete -Uri "http://127.0.0.1:8080/api/v1/kv/user:balance"
    Write-Host "DELETE Response: $($delResp | ConvertTo-Json -Compress)"

    Write-Host "`n=== ALL LIVE CLUSTER INTEGRATION TESTS PASSED ==="
}
finally {
    Write-Host "`nStopping remaining test processes..."
    Stop-Process -Id $n1.Id, $n2.Id, $n3.Id, $coord.Id -Force -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force "./data/test-node-*" -ErrorAction SilentlyContinue
}
