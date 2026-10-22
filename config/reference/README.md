# Reference data

Small lookup tables used by the Tier 1 rules. Data owners maintain these; no code change
is needed to extend them.

| File | Used for |
|---|---|
| `countries.csv` | country aliases -> ISO 3166-1 alpha-2 (`India`, `IND`, `Bharat` -> `IN`) |
| `calling_codes.csv` | ISO country -> calling code for E.164 phone numbers |
| `email_domain_typos.csv` | common domain typos (`gmial.com` -> `gmail.com`) |
| `placeholders.csv` | values that mean "no data" (`test@test.com`, `9999999999`, `Walk In`) |
| `nicknames.csv` | nickname -> canonical first name for name comparison |

**Nicknames:** only add true diminutives. Do not map a name that is also a full given
name in its own right (for example `priya` -> `priyanka`): that makes two different
people look like one, and makes a typo of the short name look like a different person.
