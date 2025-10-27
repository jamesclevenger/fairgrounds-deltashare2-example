@description('The name prefix for all resources')
param namePrefix string = 'fairgrounds-deltashare'

@description('The location for all resources')
param location string = resourceGroup().location

@description('The environment (development, staging, production)')
@allowed(['development', 'staging', 'production'])
param environment string = 'development'

@description('Delta Sharing Bearer Token')
@secure()
param bearerToken string

@description('MinIO root user')
param minioRootUser string = 'minioadmin'

@description('MinIO root password')
@secure()
param minioRootPassword string = 'minioadmin123'

@description('Container image tag')
param imageTag string = 'latest'

var resourceNamePrefix = '${namePrefix}-${environment}'
var acrName = replace('${namePrefix}acr${environment}', '-', '')

// Azure Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: true
    publicNetworkAccess: 'Enabled'
    networkRuleBypassOptions: 'AzureServices'
  }
}

// MinIO Container Instance
resource minioContainerGroup 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: '${resourceNamePrefix}-minio'
  location: location
  properties: {
    containers: [
      {
        name: 'minio'
        properties: {
          image: '${acr.properties.loginServer}/minio-with-data:${imageTag}'
          ports: [
            {
              port: 9000
              protocol: 'TCP'
            }
            {
              port: 9001
              protocol: 'TCP'
            }
          ]
          environmentVariables: [
            {
              name: 'MINIO_ROOT_USER'
              value: minioRootUser
            }
            {
              name: 'MINIO_ROOT_PASSWORD'
              secureValue: minioRootPassword
            }
          ]
          command: [
            'server'
            '/data'
            '--console-address'
            ':9001'
          ]
          resources: {
            requests: {
              cpu: 1
              memoryInGB: 2
            }
          }
        }
      }
    ]
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.name
        password: acr.listCredentials().passwords[0].value
      }
    ]
    ipAddress: {
      type: 'Public'
      ports: [
        {
          port: 9000
          protocol: 'TCP'
        }
        {
          port: 9001
          protocol: 'TCP'
        }
      ]
      dnsNameLabel: '${resourceNamePrefix}-minio'
    }
    osType: 'Linux'
    restartPolicy: 'Always'
  }
}

// Delta Sharing Server Container Instance
resource deltaShareContainerGroup 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: '${resourceNamePrefix}-deltashare'
  location: location
  properties: {
    containers: [
      {
        name: 'delta-sharing-server'
        properties: {
          image: '${acr.properties.loginServer}/delta-sharing-server:${imageTag}'
          ports: [
            {
              port: 8080
              protocol: 'TCP'
            }
          ]
          environmentVariables: [
            {
              name: 'DELTA_SHARING_BEARER_TOKEN'
              secureValue: bearerToken
            }
          ]
          resources: {
            requests: {
              cpu: 1
              memoryInGB: 2
            }
          }
        }
      }
    ]
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.name
        password: acr.listCredentials().passwords[0].value
      }
    ]
    ipAddress: {
      type: 'Public'
      ports: [
        {
          port: 8080
          protocol: 'TCP'
        }
      ]
      dnsNameLabel: '${resourceNamePrefix}-deltashare'
    }
    osType: 'Linux'
    restartPolicy: 'Always'
  }
}

// Outputs
output acrLoginServer string = acr.properties.loginServer
output acrName string = acr.name
output minioUrl string = 'http://${minioContainerGroup.properties.ipAddress.fqdn}:9000'
output minioConsoleUrl string = 'http://${minioContainerGroup.properties.ipAddress.fqdn}:9001'
output deltaShareUrl string = 'http://${deltaShareContainerGroup.properties.ipAddress.fqdn}:8080'
output deltaShareTestCommand string = 'curl -H "Authorization: Bearer ${bearerToken}" ${deltaShareContainerGroup.properties.ipAddress.fqdn}:8080/shares'