"""Data before Chaos: customer data harmonization and quality guardrails on Microsoft Fabric.

The package holds all data logic so it can be unit tested locally and shipped to Fabric
as a wheel. Fabric notebooks stay thin: they build a :class:`RunContext` and call a job
function from :mod:`goldenrecord.jobs`.
"""

__version__ = "0.1.0"
