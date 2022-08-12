# PhotonDB

This document describes the top-level design of PhotonDB.

## Overview

PhotonDB is a distributed data store.

PhotonDB disaggregates compute and storage to provide a scalable and cost-effective service.

## Data model

A PhotonDB deployment is called a universe.

PhotonDB supports ordered and unordered collections.

## Architecture

![Architecture](media/architecture.drawio.svg)

PhotonDB employs a three-tier architecture:

- The service tier consists of a set of route nodes.
- The compute tier consists of a set of group nodes.
- The storage tier consists of a set of shard nodes.

## Group

![Group](media/group.drawio.svg)

A group consists of multiple replicas that run the Raft concensus protocol.

A group manages one or more shards.

## Future goals

### Multi-tenant