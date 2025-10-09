#!/bin/bash

# Docker Network Cleanup Script for Gemini
# This script cleans up Docker networks and containers that might be causing conflicts

echo "🧹 Cleaning up Gemini Docker resources..."

# Stop and remove all gemini containers
echo "Stopping Gemini containers..."
docker stop gemini-oracle gemini-test gemini-test-1 gemini-test-2 gemini-test-3 2>/dev/null || true
docker rm gemini-oracle gemini-test gemini-test-1 gemini-test-2 gemini-test-3 2>/dev/null || true

# Remove Docker Compose stacks
echo "Removing Docker Compose stacks..."
docker compose -f docker/docker-compose-scylla.yml down --volumes --remove-orphans 2>/dev/null || true
docker compose -f docker/docker-compose-scylla-cluster.yml down --volumes --remove-orphans 2>/dev/null || true
docker compose -f docker/docker-compose-cassandra.yml down --volumes --remove-orphans 2>/dev/null || true

# Remove conflicting networks
echo "Removing potentially conflicting networks..."
docker network rm gemini gemini-single gemini-cluster 2>/dev/null || true

# Clean up unused networks and volumes
echo "Cleaning up unused Docker resources..."
docker network prune -f
docker volume prune -f

# List remaining networks for verification
echo "📋 Remaining Docker networks:"
docker network ls

echo "✅ Cleanup complete! You can now run 'make scylla-setup' safely."
