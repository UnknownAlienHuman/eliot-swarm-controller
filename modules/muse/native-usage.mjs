// Runtime validation mirrors the installed SDK 1.3.0 SubscriptionUsage
// interface. The SDK ships this wire shape as TypeScript declarations only.
const hasOwn = (value, key) => Object.hasOwn(value, key);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const integer = (value, minimum = 0) => Number.isSafeInteger(value) && value >= minimum;
const COLLECTION_ISSUES = new Set(['read_unavailable', 'read_unsupported', 'invalid_payload']);
function checkedCollectionIssue(value) {
  if (value === null || COLLECTION_ISSUES.has(value)) return value;
  throw new Error('NATIVE_USAGE_COLLECTION_ISSUE_INVALID');
}
const text = value => typeof value === 'string' && Buffer.byteLength(value, 'utf8') > 0
  && Buffer.byteLength(value, 'utf8') <= 256 && !/\p{Cc}/u.test(value);

export function validateSubscriptionUsage(value) {
  if (!object(value) || !hasOwn(value, 'observedAtMs') || !integer(value.observedAtMs)
      || !hasOwn(value, 'tier') || !text(value.tier)
      || !object(value.weekly) || !integer(value.weekly.resetsAtMs)
      || !integer(value.weekly.usedPercent)
      || !object(value.window) || !integer(value.window.resetsAtMs)
      || !integer(value.window.usedPercent)
      || !integer(value.window.windowDurationMins, 1)) return null;

  return {
    observedAtMs: value.observedAtMs,
    tier: value.tier,
    weekly: { resetsAtMs: value.weekly.resetsAtMs, usedPercent: value.weekly.usedPercent },
    window: {
      resetsAtMs: value.window.resetsAtMs,
      usedPercent: value.window.usedPercent,
      windowDurationMins: value.window.windowDurationMins,
    },
  };
}

export function emptyNativeUsageSnapshot({ connectionId, method, revision, collectedAtMs, completeness, collectionIssue = null }) {
  return {
    schema_version: 1,
    service: 'muse',
    connection_id: connectionId,
    auth_context_ref: null,
    collected_at_ms: collectedAtMs,
    freshness: 'current',
    completeness,
    collection_issue: checkedCollectionIssue(collectionIssue),
    evidence: { method, revision, native_observed_at_ms: null },
    ordinary_usage_allowed: null,
    buckets: [],
  };
}

export function nativeUsageSnapshot({ connectionId, method, revision, collectedAtMs, value }) {
  const usage = validateSubscriptionUsage(value);
  const snapshot = emptyNativeUsageSnapshot({
    connectionId, method, revision, collectedAtMs,
    completeness: usage ? 'full' : 'invalid',
  });
  if (!usage) {
    if (integer(value?.observedAtMs)) snapshot.evidence.native_observed_at_ms = value.observedAtMs;
    return snapshot;
  }

  snapshot.evidence.native_observed_at_ms = usage.observedAtMs;
  snapshot.buckets = [{
    id: 'subscription',
    name: null,
    normal_model_slug: null,
    plan_type: { value: usage.tier, collected_at_ms: collectedAtMs },
    primary: {
      collected_at_ms: collectedAtMs,
      used_percent: usage.window.usedPercent,
      resets_at_ms: usage.window.resetsAtMs,
      window_duration_mins: usage.window.windowDurationMins,
    },
    secondary: {
      collected_at_ms: collectedAtMs,
      used_percent: usage.weekly.usedPercent,
      resets_at_ms: usage.weekly.resetsAtMs,
      window_duration_mins: null,
    },
    provider_condition: null,
    credits: null,
    individual_limit: null,
    spend_control_reached: null,
    rate_limit_reached_type: null,
  }];
  return snapshot;
}

export function nativeUsageReadSnapshot({ connectionId, revision, collectedAtMs, result }) {
  if (!object(result)) {
    return emptyNativeUsageSnapshot({
      connectionId, method: 'usage/read', revision, collectedAtMs, completeness: 'invalid',
    });
  }
  if (!hasOwn(result, 'usage')) {
    return emptyNativeUsageSnapshot({
      connectionId, method: 'usage/read', revision, collectedAtMs, completeness: 'not_observed',
    });
  }
  return nativeUsageSnapshot({
    connectionId, method: 'usage/read', revision, collectedAtMs, value: result.usage,
  });
}

export function restoredNativeUsageSnapshot(snapshot) {
  if (!object(snapshot)) return undefined;
  return {
    ...snapshot,
    buckets: Array.isArray(snapshot.buckets)
      ? snapshot.buckets.map(bucket => object(bucket) ? { ...bucket, provider_condition: null } : bucket)
      : snapshot.buckets,
    ordinary_usage_allowed: null,
    collection_issue: null,
    freshness: 'stale',
  };
}

export function closedNativeUsageSnapshot(snapshot, { connectionId, revision, collectedAtMs }) {
  if (object(snapshot) && snapshot.connection_id === connectionId) {
    return {
      ...snapshot,
      collected_at_ms: collectedAtMs,
      buckets: Array.isArray(snapshot.buckets)
        ? snapshot.buckets.map(bucket => object(bucket) ? { ...bucket, provider_condition: null } : bucket)
        : snapshot.buckets,
      ordinary_usage_allowed: null,
      collection_issue: null,
      freshness: 'stale',
      evidence: { ...snapshot.evidence, method: 'connection/closed', revision },
    };
  }
  const closed = emptyNativeUsageSnapshot({
    connectionId, method: 'connection/closed', revision, collectedAtMs, completeness: 'not_observed',
  });
  closed.freshness = 'stale';
  return closed;
}
