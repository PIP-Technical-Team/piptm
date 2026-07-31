---
date: 2026-07-31
title: "API Deployment Documentation"
status: decided
scope: "Lightweight"
chosen-approach: "README Section"
tags: [documentation, deployment, azure, devops]
---

# API Deployment Documentation

## Context

Currently, deployment knowledge for the Table Maker API exists only in one person's head. The team needs this documented so anyone can deploy to DEV/QA/PROD environments. Deployment involves running three Azure DevOps pipelines in sequence, with some conditional logic about when to run the data copy pipeline.

## Requirements

### Purpose
Knowledge transfer — document deployment workflow so PIP team developers can deploy the API without tribal knowledge dependency.

### Users
PIP team developers who need to deploy or maintain the API service.

### Deployment Workflow
Three Azure DevOps pipelines per environment (DEV, QA, PROD):

1. **Copy data to cloud storage** — uploads Arrow/Parquet data and manifests
2. **Build pipeline** — prepares Docker image  
3. **Release pipeline** — builds Docker image, deploys to Azure, mounts cloud storage, starts container (auto-triggered by build)

**Execution order**:
- Copy data pipeline: Only run when new data available or first-time cloud storage mount
- Build pipeline: Run for all deployments (code changes, new releases)
- Release pipeline: Auto-triggered by build pipeline completion

### Prerequisites
- Azure DevOps access to PIP project (no special permissions required)
- Data preparation already complete (out of scope — separate future documentation)

### Verification
- Health check endpoints
- Insomnia collection testing

### Constraints
- Keep troubleshooting minimal for now (can expand later based on real failure patterns)
- No timing delays between pipeline steps needed

## Approaches Considered

### Approach 1: Task-Oriented Single Vignette
Create a comprehensive vignette organized by deployment tasks (initial setup, routine deployment, verification).

**Pros**:
- Clear workflow-based navigation
- Distinguishes first-time vs routine deployment  
- Single discoverable location in pkgdown
- Proper structure for detailed narrative

**Cons**:
- Vignette infrastructure overhead for operational docs
- Mixed conceptual/procedural content
- Not where developers look first for deployment info

**Effort**: Small (2–3 hours)

### Approach 2: Reference-Style Multi-Section Vignette
One vignette as a reference guide with pipeline catalog, environment matrix, decision trees.

**Pros**:
- Easy to scan for specific pipeline info
- Environment matrix shows all links at glance
- Decision tree helps newcomers

**Cons**:
- Over-engineered for current simplicity (3 pipelines)
- More structure than content warrants
- Higher maintenance overhead
- Still vignette infrastructure overhead

**Effort**: Medium (3–4 hours)

### Approach 3: README Section (CHOSEN)
Add "Deployment" section to existing README.md with workflow, when-to-run guidance, and verification steps.

**Pros**:
- Developers look at README first
- No vignette build/infrastructure overhead
- Operational knowledge stays with operational docs
- Easy to update without package rebuild
- Quick to implement

**Cons**:
- README could become long (mitigated: deployment is ~1 section)
- Less structured than vignette format (acceptable for this content)

**Effort**: Small (1–2 hours)

## Decision

**Chosen: Approach 3 — README Section**

Add deployment documentation as a new section in `README.md` rather than creating a vignette. 

**Rationale**:
- This is operational/deployment knowledge, not usage examples or tutorials
- README is the first place developers look when they need to deploy
- No vignette infrastructure overhead
- Faster to write and easier to maintain
- Can always promote to vignette later if content grows substantially

## Next Steps

1. Add "Deployment" section to `README.md` with:
   - Overview of three pipelines (copy, build, release)
   - Execution order and auto-trigger behavior
   - When to run copy data pipeline vs skip
   - Prerequisites (Azure DevOps access)
   - Verification steps (health endpoints, Insomnia tests)
   - Links to Azure DevOps pipelines for DEV/QA/PROD environments
   
2. Add "Out of Scope" note linking to future data preparation documentation

3. Consider adding environment variable documentation if not already present
