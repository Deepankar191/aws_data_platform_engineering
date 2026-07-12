# Sample data — cross-source linkage

These files are a tiny, self-consistent slice of the four sources in
[`docs/SPEC.md` §2](../docs/SPEC.md). They exist so a reviewer can trace a single
customer end-to-end through ingestion → identity resolution (§6) → decision input (§5),
and so the identity/conflict logic has real matches (and one real conflict) to chew on.

## The 7 customers

`customer_profile_seed.sql` is the **identity spine**: `internal_customer_uuid` is the
canonical id (SPEC.md §6.1). Every other source links back to it through a native key.

| # | internal_customer_uuid | full_name | date_of_birth | emirates_id (→ AECB) | phone (→ Fraud) | email (→ Fraud) |
|---|---|---|---|---|---|---|
| 1 | `1111…1111` | Ahmed Al Mansoori | 1988-03-14 | 784-1988-1234567-1 | +971501234567 | ahmed.almansoori@example.ae |
| 2 | `2222…2222` | Fatima Hassan | 1992-07-22 | 784-1992-2345678-2 | +971502345678 | fatima.hassan@example.ae |
| 3 | `3333…3333` | Rajesh Kumar | 1985-11-30 | 784-1985-3456789-3 | **+971503456789** | rajesh.kumar@example.ae |
| 4 | `4444…4444` | Sara Abdullah | 1995-01-09 | 784-1995-4567890-4 | +971504567890 | sara.abdullah@example.ae |
| 5 | `5555…5555` | Mohammed Ali | 1979-06-18 | 784-1979-5678901-5 | +971505678901 | mohammed.ali@example.ae |
| 6 | `6666…6666` | Priya Nair | 1990-09-03 | 784-1990-6789012-6 | +971506789012 | priya.nair@example.ae |
| 7 | `7777…7777` | Omar Sheikh | 1983-12-25 | 784-1983-7890123-7 | +971507890123 | omar.sheikh@example.ae |

## How each source links to the spine (SPEC.md §6)

| Source | File | Match key(s) | Match type |
|---|---|---|---|
| Internal profile (spine) | `postgres/customer_profile_seed.sql` | `internal_customer_uuid` | canonical |
| AECB credit report | `aecb/aecb_report_batch_20250401.xml` | `emirates_id` (normalised) | deterministic |
| Fraud scoring | `fraud/fraud_events_20250401.json` | `phone` + `email` | deterministic when both match; else probabilistic |
| AML/PEP screening | `aml/aml_callbacks_20250401.json` | `soundex(full_name)` + `date_of_birth` | probabilistic |

Every customer appears in **all four** sources, so each spine row should resolve a full
`decision_input` with AECB + fraud + AML + profile inputs all present.

## The two intentional wrinkles (so §6 is demonstrable)

1. **Phone conflict — customer 3, Rajesh Kumar.**
   The profile phone is `+971503456789`, but the fraud feed (`FRD-20250401-0003`) reports
   `+971503456700`. Email still matches. So the fraud row matches on **email only**, which
   per §6.3 makes it a *candidate* resolved by the probabilistic scorer rather than an exact
   two-key deterministic match. Under `SURVIVORSHIP_PRIORITY = [POSTGRES, AECB, FRAUD, AML]`
   the POSTGRES phone wins the golden record. This is the conflict-resolution demo.

2. **Fuzzy surname — customer 7, Omar Sheikh.**
   AML (`AML-20250401-0007`) spells the surname **`Shaikh`** vs the profile's **`Sheikh`**.
   `soundex("Omar Shaikh") == soundex("Omar Sheikh")` and the DOB matches, so this exercises
   the fuzzy `soundex(name)+dob` path (§6.4) and the probabilistic scorer / Jaro-Winkler
   name similarity above `MATCH_THRESHOLD = 0.85`.

## Other DQ-relevant signals baked in

- Customer 5 (Mohammed Ali): AML `HIT` **and** `is_pep = true` — exercises PEP exposure.
- Customer 3 (Rajesh Kumar): AML `PENDING` — exercises the WARN "AML PENDING rate ≤ 5%" rule (§8).
- Customer 7 (Omar Sheikh): fraud `DECLINE` (score 0.889) — high-risk outcome.
- Customers 4 and 7: `kyc_completed = FALSE`.
- AECB scores span 573–811, all inside the valid 300–900 MUST-PASS band (§8).

## Running the sample through the platform

- **Profile (CDC):** load `postgres/customer_profile_seed.sql` into the local Postgres from
  `ingestion/debezium/docker-compose.yml`; Debezium emits CDC to Kafka → S3 sink → `bronze/customer_profile/`.
- **AECB:** drop `aecb/aecb_report_batch_20250401.xml` on the SFTP server; `ingestion/aecb_sftp/aecb_sftp_ingest.py` parses it to `bronze/aecb/`.
- **Fraud:** point `ingestion/fraud_api/fraud_api_ingest.py` at a mock returning `fraud/fraud_events_20250401.json`; lands in `bronze/fraud/`.
- **AML:** POST each element of `aml/aml_callbacks_20250401.json` to the API Gateway from `ingestion/aml_webhook/`; the Lambda lands them in `bronze/aml/`.
