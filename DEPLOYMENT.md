# Deployment Guide

This guide covers deploying the Fairgrounds Deltashare example to Microsoft Azure using GitHub Actions.

## Prerequisites

### Azure Setup
1. **Azure Subscription**: Ensure you have an active Azure subscription
2. **Azure CLI**: Install Azure CLI for local testing (optional)
3. **Resource Group**: The GitHub Action will create this automatically

### GitHub Repository Setup
1. **Fork/Clone**: Fork this repository or push to your own GitHub repository
2. **GitHub Secrets**: Configure required secrets (see below)

## Required GitHub Secrets

Configure the following secrets in your GitHub repository (Settings → Secrets and variables → Actions):

### `AZURE_CREDENTIALS`
Create an Azure Service Principal with Contributor role:

```bash
# Login to Azure
az login

# Create service principal
az ad sp create-for-rbac \
  --name "fairgrounds-deltashare-sp" \
  --role contributor \
  --scopes /subscriptions/{subscription-id} \
  --sdk-auth
```

Copy the entire JSON output as the `AZURE_CREDENTIALS` secret.

### `DELTA_SHARING_BEARER_TOKEN`
Generate a secure bearer token for Delta Sharing authentication:

```bash
# Generate secure random token
openssl rand -hex 32
```

Use this value as the `DELTA_SHARING_BEARER_TOKEN` secret.

## Deployment Process

### Manual Deployment via GitHub Actions

1. **Navigate to Actions**: Go to your GitHub repository → Actions tab
2. **Select Workflow**: Choose "Deploy to Azure Container Instances"
3. **Run Workflow**: Click "Run workflow" button
4. **Configure Options**:
   - **Environment**: Choose `development` or `production`
   - **Bearer Token**: Optionally override the default token from secrets
5. **Execute**: Click "Run workflow" to start deployment

### Deployment Stages

The GitHub Action performs these steps:

1. **Setup**: Checkout code and login to Azure
2. **Infrastructure**: Create Resource Group and Azure Container Registry
3. **Build Images**: 
   - Build custom Delta Sharing server image
   - Build MinIO image with sample data
4. **Push Images**: Push both images to Azure Container Registry
5. **Deploy Containers**: 
   - Deploy MinIO container instance
   - Deploy Delta Sharing server container instance
6. **Output URLs**: Display connection information

## Post-Deployment

### Access Your Deployment

After successful deployment, you'll receive URLs for:

- **Delta Sharing Server**: `http://fairgrounds-deltashare-{env}-deltashare.{location}.azurecontainer.io:8080`
- **MinIO Console**: `http://fairgrounds-deltashare-{env}-minio.{location}.azurecontainer.io:9001`
- **MinIO API**: `http://fairgrounds-deltashare-{env}-minio.{location}.azurecontainer.io:9000`

### Test Your Deployment

```bash
# Test Delta Sharing API
curl -H "Authorization: Bearer YOUR_BEARER_TOKEN" \
  http://fairgrounds-deltashare-{env}-deltashare.{location}.azurecontainer.io:8080/shares

# Expected response: {"items":[{"name":"fairgrounds_share","id":"fairgrounds_share"}]}
```

### MinIO Access

1. Open MinIO Console URL in browser
2. Login with credentials:
   - **Username**: `minioadmin`
   - **Password**: `minioadmin123`
3. Browse sample data in `/data/sample_data/` folder

## Environment Management

### Development Environment
- Uses basic resource sizing
- Includes debug logging
- Uses development-specific DNS names

### Production Environment
- Recommended for production workloads
- Uses production-hardened configurations
- Requires production-grade bearer tokens

## Infrastructure as Code (Optional)

The project includes Azure Bicep templates for advanced deployment scenarios:

```bash
# Deploy using Bicep
az deployment group create \
  --resource-group fairgrounds-deltashare-rg \
  --template-file azure/main.bicep \
  --parameters @azure/parameters.development.json
```

## Troubleshooting

### Common Issues

**Authentication Errors**:
- Verify `AZURE_CREDENTIALS` secret is correctly formatted JSON
- Ensure service principal has Contributor role
- Check subscription ID in service principal scope

**Container Startup Failures**:
- Check Azure Container Instance logs in Azure Portal
- Verify bearer token format and security
- Ensure resource quotas are sufficient

**Network Connectivity**:
- Confirm container instances have public IP addresses
- Check Azure Network Security Groups if custom networking
- Verify DNS resolution for container FQDNs

### Cleanup Resources

To remove all deployed resources:

```bash
# Delete entire resource group
az group delete --name fairgrounds-deltashare-rg --yes --no-wait
```

## Security Considerations

### Production Deployment
- Use strong, unique bearer tokens
- Consider Azure Private DNS for internal-only access
- Implement network security groups for traffic filtering
- Enable Azure Container Registry vulnerability scanning
- Use Azure Key Vault for secret management

### Monitoring
- Enable Azure Container Insights for monitoring
- Set up Azure Log Analytics for centralized logging
- Configure alerts for container health and performance

## Cost Management

### Resource Costs
- **Azure Container Registry**: ~$5/month (Basic tier)
- **Azure Container Instances**: ~$30-50/month (2 instances, 1 CPU, 2GB RAM each)
- **Data Transfer**: Varies based on usage

### Cost Optimization
- Use development environment for testing
- Stop container instances when not needed
- Monitor usage with Azure Cost Management

## Support

For deployment issues:
1. Check GitHub Actions logs for detailed error messages
2. Review Azure Portal for resource status and logs
3. Consult Azure Container Instances documentation
4. Open GitHub issue with deployment logs and error details