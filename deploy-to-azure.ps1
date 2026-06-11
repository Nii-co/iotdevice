<#
.SYNOPSIS
    将 IoT 设备模拟器部署到 Azure Container Instance (ACI) — DPS + 对称密钥组方式 (Azure 国际版)

.DESCRIPTION
    一键完成: 创建资源组 → 创建 ACR → 构建镜像 → 推送 → 部署 ACI
    设备通过 DPS 对称密钥 Enrollment Group 注册, 不再需要 iothubowner。
    DPS ID Scope 与组主密钥通过 ACI 安全环境变量注入。

.PARAMETER ResourceGroup
    Azure 资源组名称

.PARAMETER Location
    Azure 区域 (国际版默认 eastasia)

.PARAMETER IdScopeSensors
    sensors-group 所属 DPS 的 ID Scope

.PARAMETER GroupKeySensors
    sensors-group 的组主密钥 (Primary Key, base64)

.PARAMETER IdScopeEnergy
    energy-group 所属 DPS 的 ID Scope (同一 DPS 时与 IdScopeSensors 相同)

.PARAMETER GroupKeyEnergy
    energy-group 的组主密钥 (Primary Key, base64)

.PARAMETER SendInterval
    发送间隔秒数 (默认 10)

.PARAMETER MessageCount
    每设备发送消息数 (0=无限, 默认 0)

.EXAMPLE
    .\deploy-to-azure.ps1 -ResourceGroup "rg-iot-simulator" `
        -IdScopeSensors "0ne00XXXXXX" -GroupKeySensors "<key>" `
        -IdScopeEnergy "0ne00XXXXXX" -GroupKeyEnergy "<key>"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroup,

    [string]$Location = "eastasia",

    [Parameter(Mandatory=$true)]
    [string]$IdScopeSensors,

    [Parameter(Mandatory=$true)]
    [string]$GroupKeySensors,

    [Parameter(Mandatory=$true)]
    [string]$IdScopeEnergy,

    [Parameter(Mandatory=$true)]
    [string]$GroupKeyEnergy,

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
# 设备走 DPS 对称密钥组注册; ID Scope 与组主密钥通过安全环境变量注入,
# 容器内 Python 由 os.environ 读取并按 registration_id 派生每台设备密钥。
Write-Host "[5/5] Deploying container instance..." -ForegroundColor Yellow

az container create `
    --resource-group $ResourceGroup `
    --name $ContainerName `
    --image "${acrServer}/${ImageName}:${ImageTag}" `
    --os-type Linux `
    --registry-login-server $acrServer `
    --registry-username $acrUser `
    --registry-password $acrPass `
    --cpu 0.5 `
    --memory 0.5 `
    --restart-policy Always `
    --environment-variables `
        "MODE=multi" `
        "CONFIG_PATH=simulator_config.json" `
        "INTERVAL=$SendInterval" `
        "COUNT=$MessageCount" `
        "PYTHONUNBUFFERED=1" `
    --secure-environment-variables `
        "DPS_IDSCOPE_SENSORS=$IdScopeSensors" `
        "DPS_GROUPKEY_SENSORS=$GroupKeySensors" `
        "DPS_IDSCOPE_ENERGY=$IdScopeEnergy" `
        "DPS_GROUPKEY_ENERGY=$GroupKeyEnergy" `
    --command-line "python iot_device_simulator.py --mode multi --config simulator_config.json --interval $SendInterval --count $MessageCount" `
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
