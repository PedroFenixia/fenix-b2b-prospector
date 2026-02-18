-- Migration: Add datos_registrales column to companies table
-- Run after deploying the code changes.

-- PostgreSQL
ALTER TABLE companies ADD COLUMN IF NOT EXISTS datos_registrales TEXT;

-- Optional: backfill from existing act text (for companies that already have
-- Datos registrales in their acts' texto_original).
-- This is best done via a re-ingestion pass rather than SQL.
