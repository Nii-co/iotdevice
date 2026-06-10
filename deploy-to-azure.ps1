<#
.SYNOPSIS
    将 IoT 设备模拟器部署到 Azure Container Instance (ACI)

.DESCRIPTION
    一键完成: 创建资源组 → 创建 ACR → 构建镜像 → 推送 → 部署 ACI

.PARAMETER ResourceGroup
    Azure 资源组名称

.PARAMETER Location
    Azure 区域 (默认 eastasia)

.PARAMETER IoTHubConnectionString
    IoT Hub 服务连接字符串 (iothubowner)

.PARAMETER SendInterval
    发送间隔秒数 (默认 10)

.PARAMETER MessageCount
    每设备发送消息数 (0=无限, 默认 0)

.EXAMPLE
    .\deploy-to-azure.ps1 -ResourceGroup "rg-iot-simulator" -IoTHubConnectionString "HostName=..."
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroup,

    [string]$Location = "eastasia",

    [Parameter(Mandatory=$true)]
    [string]$IoTHubConnectionString,

    [int]$SendInterval = 10,

    [int]$MessageCount = 0,

    [string]$AcrName = ""
)

$ErrorActionPreference = "Stop"

# 自动生成 ACR 名称 (只允许字母数字)
if (-not $AcrName) {
    $suffix = -join ((48..57) + (97..122) | Get-Random -Count 6 | ForEach-Object { [char]$_ })
    $AcrName = "acriotsim$suffix"
}

$ImageName = "iot-device-simulator"
$ImageTag = "latest"
$ContainerName = "iot-simulator"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " IoT Device Simulator - Azure Deployment"    -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Resource Group : $ResourceGroup"
Write-Host "  Location       : $Location"
Write-Host "  ACR            : $AcrName"
Write-Host "  Container      : $ContainerName"
Write-Host "  Interval       : ${SendInterval}s"
Write-Host "  Count          : $(if ($MessageCount -eq 0) { 'unlimited' } else { $MessageCount })"
Write-Host ""

# Step 1: 创建资源组
Write-Host "[1/5] Creating resource group..." -ForegroundColor Yellow
az group create --name $ResourceGroup --location $Location --output none

# Step 2: 创建 ACR
Write-Host "[2/5] Creating Azure Container Registry: $AcrName ..." -ForegroundColor Yellow
az acr create --resource-group $ResourceGroup --name $AcrName --sku Basic --output none
az acr update --name $AcrName --admin-enabled true --output none

# Step 3: 构建镜像 (ACR Build, 无需本地 Docker)
Write-Host "[3/5] Building container image in ACR (cloud build)..." -ForegroundColor Yellow
az acr build --registry $AcrName --image "${ImageName}:${ImageTag}" --file Dockerfile . --output none

# Step 4: 获取 ACR 凭据
Write-Host "[4/5] Retrieving ACR credentials..." -ForegroundColor Yellow
$acrServer = az acr show --name $AcrName --query loginServer -o tsv
$acrUser   = az acr credential show --name $AcrName --query username -o tsv
$acrPass   = az acr credential show --name $AcrName --query "passwords[0].value" -o tsv

# Step 5: 部署 ACI
Write-Host "[5/5] Deploying container instance..." -ForegroundColor Yellow

# 先更新配置文件中的连接字符串 (注入到镜像运行时通过环境变量无法直接覆盖 JSON)
# 所以我们通过 ACI 的环境变量 + 修改启动命令来传递

az container create `
    --resource-group $ResourceGroup `
    --name $ContainerName `
    --image "${acrServer}/${ImageName}:${ImageTag}" `
    --registry-login-server $acrServer `
    --registry-username $acrUser `
    --registry-password $acrPass `
    --cpu 0.5 `
    --memory 0.5 `
    --restart-policy Always `
    --environment-variables `
        "IOTHUB_CONNECTION_STRING=$IoTHubConnectionString" `
        "MODE=multi" `
        "INTERVAL=$SendInterval" `
        "COUNT=$MessageCount" `
    --command-line "python iot_device_simulator.py --mode multi --interval $SendInterval --count $MessageCount" `
    --output none

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Deployment Complete!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "  View logs:"
Write-Host "    az container logs --resource-group $ResourceGroup --name $ContainerName --follow"
Write-Host ""
Write-Host "  Stop:"
Write-Host "    az container stop --resource-group $ResourceGroup --name $ContainerName"
Write-Host ""
Write-Host "  Restart:"
Write-Host "    az container start --resource-group $ResourceGroup --name $ContainerName"
Write-Host ""
Write-Host "  Delete all resources:"
Write-Host "    az group delete --name $ResourceGroup --yes --no-wait"
Write-Host ""
