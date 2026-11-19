Write-Host "Starting miniKV 3-node cluster + coordinator..." -ForegroundColor Cyan

# Create data directories
New-Item -ItemType Directory -Force -Path "data/node-1", "data/node-2", "data/node-3" | Out-Null

$node1 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-1 --addr 127.0.0.1:7000 --http-addr 127.0.0.1:7001 --data-dir ./data/node-1" -PassThru
$node2 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-2 --addr 127.0.0.1:7010 --http-addr 127.0.0.1:7011 --data-dir ./data/node-2" -PassThru
$node3 = Start-Process -FilePath ".\minikv-node.exe" -ArgumentList "--id node-3 --addr 127.0.0.1:7020 --http-addr 127.0.0.1:7021 --data-dir ./data/node-3" -PassThru

Start-Sleep -Seconds 1

$coord = Start-Process -FilePath ".\minikv-coord.exe" -ArgumentList '--addr :8080 --nodes "node-1=127.0.0.1:7000=127.0.0.1:7001,node-2=127.0.0.1:7010=127.0.0.1:7011,node-3=127.0.0.1:7020=127.0.0.1:7021"' -PassThru

Write-Host "miniKV Cluster is online!" -ForegroundColor Green
Write-Host "Dashboard reachable at: http://localhost:8080" -ForegroundColor Yellow
Write-Host "Press Enter to stop all nodes..."
Read-Host

Stop-Process -Id $node1.Id, $node2.Id, $node3.Id, $coord.Id -Force
Write-Host "Cluster stopped."
