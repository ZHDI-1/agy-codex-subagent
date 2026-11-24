/**
 * Unified Subagent Mission Control Frontend (`app.js`).
 * Drives live inspection, mid-turn steering (`turn/steer` & `SendAgentMessage`),
 * unified diff viewing, plan checklists, and cross-engine handoff for both
 * OpenAI Codex and native Gemini subagents.
 */

const state = {
  subagents: [],
  selectedId: null,
  activeRootConvId: '',
  currentOnly: false,
  activeTab: 'stream',
  inspected: null,
};

const el = (id) => document.getElementById(id);

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function showFeedback(text, isError = false) {
  const fb = el('steer-feedback');
  if (!fb) return;
  fb.textContent = text;
  fb.style.color = isError ? '#f87171' : '#34d399';
  setTimeout(() => {
    if (fb.textContent === text) fb.textContent = '';
  }, 4000);
}

async function apiGet(path, params = {}) {
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(qs ? `${path}?${qs}` : path);
  return await res.json();
}

async function apiPost(path, body = {}) {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return await res.json();
}

function renderRoster() {
  const container = el('roster-list');
  const filtered = state.currentOnly && state.activeRootConvId
    ? state.subagents.filter((s) => s.matchesActiveConversation)
    : state.subagents;

  el('roster-count').textContent = `${filtered.length} subagent${filtered.length === 1 ? '' : 's'}`;

  if (!filtered.length) {
    state.selectedId = null;
    renderInspector(null);
    container.innerHTML = `<div class="mc-empty">No subagents found${state.currentOnly ? ' for current conversation' : ''}. Invoke <code>codex</code>, <code>self</code>, or <code>research</code>, or click <b>+ New Codex</b>.</div>`;
    return;
  }

  if (!state.selectedId || !filtered.some((s) => s.conversationId === state.selectedId)) {
    state.selectedId = filtered[0].conversationId;
  }

  container.innerHTML = filtered
    .map((s) => {
      const isSelected = s.conversationId === state.selectedId;
      const engineClass = s.engine === 'codex' ? 'engine-codex' : 'engine-gemini';
      const engineLabel = s.engine === 'codex' ? `CODEX · ${s.effort || 'high'}` : `GEMINI · ${s.agentName || 'sub'}`;
      const statusClass = `status-${s.status || 'idle'}`;
      const activity = s.activity || (s.status === 'working' ? 'Working…' : 'Idle');
      return `
        <div class="roster-card ${isSelected ? 'selected' : ''}" data-conv-id="${escapeHtml(s.conversationId)}">
          <div class="roster-top">
            <div class="roster-left">
              <span class="engine-pill ${engineClass}">${escapeHtml(engineLabel)}</span>
              <span class="roster-role">${escapeHtml(s.role || s.conversationId.slice(0, 12))}</span>
            </div>
            <span class="status-pill ${statusClass}">${escapeHtml(s.status || 'idle')}</span>
          </div>
          <div class="roster-bottom">
            <span class="roster-activity">${escapeHtml(activity)}</span>
            <span>${Number(s.steps || 0)} steps</span>
          </div>
        </div>
      `;
    })
    .join('');

  for (const card of container.querySelectorAll('.roster-card')) {
    card.addEventListener('click', () => {
      state.selectedId = card.getAttribute('data-conv-id');
      renderRoster();
      pollInspect();
    });
  }
}

function formatUnifiedDiff(diffText) {
  if (!diffText || !diffText.trim()) {
    return 'No unified diff recorded yet for this subagent.';
  }
  return diffText
    .split('\n')
    .map((line) => {
      const esc = escapeHtml(line);
      if (line.startsWith('+++') || line.startsWith('---') || line.startsWith('@@') || line.startsWith('diff --git')) {
        return `<span class="diff-hdr">${esc}</span>`;
      }
      if (line.startsWith('+')) {
        return `<span class="diff-add">${esc}</span>`;
      }
      if (line.startsWith('-')) {
        return `<span class="diff-del">${esc}</span>`;
      }
      return esc;
    })
    .join('\n');
}

function renderInspector(data) {
  const pane = el('inspector-pane');
  const hasSession = Boolean(data?.summary);
  for (const section of pane.querySelectorAll('.inspector-header, .mc-steer-bar, .session-controls')) {
    section.classList.toggle('hidden', !hasSession);
  }
  if (!hasSession) {
    el('approval-banner').classList.add('hidden');
    return;
  }

  const s = data.summary;
  const tr = data.transcript || {};

  const engineBadge = el('insp-engine-badge');
  engineBadge.textContent = s.engine === 'codex' ? `CODEX (${s.model || 'daemon'})` : `GEMINI (${s.agentName || 'native'})`;
  engineBadge.className = `engine-pill ${s.engine === 'codex' ? 'engine-codex' : 'engine-gemini'}`;

  el('insp-role').textContent = s.role || s.conversationId;
  const stPill = el('insp-status-pill');
  stPill.textContent = s.status || 'idle';
  stPill.className = `status-pill status-${s.status || 'idle'}`;

  const shortConv = (s.conversationId || '').slice(0, 14);
  const shortThread = s.threadId ? ` | thread: ${s.threadId.slice(0, 14)}` : '';
  const shortTurn = s.activeTurnId ? ` | turn: ${s.activeTurnId.slice(0, 10)}` : '';
  const permInfo = s.engine === 'codex'
    ? ` | perm: ${s.permissionSource || 'codexConfig'} (${s.approvalPolicy || 'never'}/${s.sandboxMode || 'danger-full-access'})`
    : '';
  el('insp-ids').textContent = `conv: ${shortConv}${shortThread}${shortTurn}${permInfo}`;

  if (s.engine === 'codex') {
    if (el('cfg-model') && !el('cfg-model').matches(':focus')) el('cfg-model').value = s.model || '';
    if (el('cfg-effort') && s.effort) el('cfg-effort').value = s.effort;
    if (el('cfg-perm-source') && s.permissionSource) el('cfg-perm-source').value = s.permissionSource;
    if (el('cfg-sandbox') && s.sandboxMode) el('cfg-sandbox').value = s.sandboxMode;
    if (el('cfg-approval') && s.approvalPolicy) el('cfg-approval').value = s.approvalPolicy;
  }

  // Pending approvals
  const approvals = s.pendingApprovals || [];
  const banner = el('approval-banner');
  if (approvals.length > 0) {
    banner.classList.remove('hidden');
    const first = approvals[0];
    el('approval-desc').textContent = `${first.method}: ${JSON.stringify(first.params || {}).slice(0, 120)}`;
  } else {
    banner.classList.add('hidden');
  }

  // Live Thinking
  const thinkingBox = el('live-thinking-box');
  const thinkingText = s.lastThinking || tr.lastThinking || '';
  if (thinkingText) {
    thinkingBox.classList.remove('hidden');
    el('live-thinking-content').textContent = thinkingText;
  } else {
    thinkingBox.classList.add('hidden');
  }

  // Steps stream
  const steps = (tr.recentSteps || []).slice().reverse();
  const stepsContainer = el('steps-container');
  if (!steps.length) {
    stepsContainer.innerHTML = `<div class="mc-empty">No steps recorded in transcript.jsonl yet.</div>`;
  } else {
    stepsContainer.innerHTML = steps
      .map((st) => {
        const toolsHtml = (st.tools || [])
          .map((t) => `<span class="tool-chip">${escapeHtml(t)}</span>`)
          .join('');
        const bodyText = st.response || st.content || st.thinking || '';
        return `
          <div class="step-card">
            <div class="step-card-header">
              <span><b>#${st.stepIndex}</b> · ${escapeHtml(st.type)} (${escapeHtml(st.status)})</span>
              <button class="btn btn-xs btn-revert-step" data-step="${st.stepIndex}" title="Revert to this step">Revert here</button>
            </div>
            ${toolsHtml ? `<div class="step-tools">${toolsHtml}</div>` : ''}
            ${bodyText ? `<div class="step-body">${escapeHtml(bodyText)}</div>` : ''}
          </div>
        `;
      })
      .join('');

    for (const btn of stepsContainer.querySelectorAll('.btn-revert-step')) {
      btn.addEventListener('click', async (ev) => {
        ev.stopPropagation();
        const stepIdx = Number(btn.getAttribute('data-step') || 1);
        const res = await apiPost('/api/subagent/revert', {
          conversationId: state.selectedId,
          stepOrTurns: s.engine === 'codex' ? 1 : stepIdx,
        });
        showFeedback(res.error ? `Revert failed: ${res.error}` : `Reverted (${res.action})`);
        pollAll();
      });
    }
  }

  // Live Diff tab
  el('diff-container').innerHTML = formatUnifiedDiff(data.diff || '');

  // Plan & Events tab
  el('plan-explanation').textContent = data.planExplanation || '';
  const planList = el('plan-list');
  const plan = data.plan || [];
  if (!plan.length) {
    planList.innerHTML = `<li class="mono-dim">No active plan steps reported yet.</li>`;
  } else {
    planList.innerHTML = plan
      .map((p) => {
        const icon = p.status === 'completed' ? '✅' : p.status === 'in_progress' ? '🔄' : '⬜';
        return `<li class="plan-item"><span>${icon}</span><span>${escapeHtml(p.step)}</span></li>`;
      })
      .join('');
  }

  const eventsList = el('events-list');
  const events = (data.events || []).slice().reverse();
  eventsList.innerHTML = events.length
    ? events
        .map(
          (ev) =>
            `<div><span class="mono-dim">${escapeHtml((ev.timestamp || '').slice(11, 19))}</span> <b>${escapeHtml(ev.type)}</b> ${escapeHtml(JSON.stringify(ev).slice(0, 110))}</div>`
        )
        .join('')
    : `<div class="mono-dim">No daemon events yet.</div>`;
}

async function pollInspect() {
  const conversationId = state.selectedId;
  if (!conversationId) return;
  try {
    const data = await apiGet('/api/subagent/inspect', { conversationId });
    if (state.selectedId !== conversationId) return;
    state.inspected = data;
    renderInspector(data);
  } catch (e) {
    console.warn('pollInspect error:', e);
  }
}

async function pollAll() {
  try {
    const res = await apiGet('/api/mission-control', { all: 'true' });
    state.subagents = res.subagents || [];
    state.activeRootConvId = res.activeRootConversationId || '';

    const badge = el('daemon-badge');
    if (res.codexDaemonConnected) {
      badge.textContent = 'Codex Daemon: Live (.sock)';
      badge.className = 'badge badge-ok';
    } else {
      badge.textContent = 'Codex Daemon: Standby';
      badge.className = 'badge badge-dim';
    }

    renderRoster();
    if (state.selectedId) {
      await pollInspect();
    }
  } catch (e) {
    console.warn('pollAll error:', e);
  }
}

// Event Listeners
const profileFields = {
  name: 'profile-name',
  description: 'profile-description',
  model: 'profile-model',
  effort: 'profile-effort',
  executionMode: 'profile-mode',
  permissionSource: 'profile-perm-source',
  sandboxMode: 'profile-sandbox',
  approvalPolicy: 'profile-approval',
  systemPrompt: 'profile-prompt',
};

function setupProfiles() {
  const selector = el('profile-select');
  const status = el('profile-feedback');
  const form = el('profile-form');
  const deleteButton = el('profile-delete');
  let profiles = {};
  let defaults = {};

  function showProfile() {
    const saved = profiles[selector.value];
    const values = saved || defaults;
    for (const [field, id] of Object.entries(profileFields)) el(id).value = values[field] ?? '';
    el('profile-name').readOnly = Boolean(saved);
    deleteButton.disabled = !saved;
  }

  async function refreshProfiles(selected = selector.value) {
    const result = await apiGet('/api/profiles');
    if (result.error) throw new Error(result.error);
    profiles = Object.fromEntries(result.profiles.map((profile) => [profile.name, profile]));
    defaults = result.defaults;
    selector.replaceChildren(new Option('New profile', ''));
    for (const name of Object.keys(profiles)) selector.add(new Option(name, name));
    selector.value = profiles[selected] ? selected : '';
    showProfile();
  }

  async function changeProfile(path, body, selected, message) {
    try {
      const result = await apiPost(path, body);
      if (result.error) throw new Error(result.error);
      await refreshProfiles(selected);
      status.textContent = message;
    } catch (error) {
      status.textContent = error.message;
    }
  }

  selector.addEventListener('change', showProfile);
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const body = Object.fromEntries(Object.entries(profileFields).map(([field, id]) => [field, el(id).value]));
    changeProfile('/api/profiles/define', body, body.name, `Saved ${body.name}.`);
  });
  deleteButton.addEventListener('click', () => {
    const name = selector.value;
    if (name && window.confirm(`Delete the Codex profile ${name}?`)) {
      changeProfile('/api/profiles/delete', { name }, '', `Deleted ${name}.`);
    }
  });
  refreshProfiles().catch((error) => { status.textContent = error.message; });
}

document.addEventListener('DOMContentLoaded', () => {
  setupProfiles();
  // Filter toggle
  el('chk-current-only').addEventListener('change', (e) => {
    state.currentOnly = e.target.checked;
    renderRoster();
  });

  // Tabs
  for (const btn of document.querySelectorAll('.tab-btn')) {
    btn.addEventListener('click', () => {
      for (const b of document.querySelectorAll('.tab-btn')) b.classList.remove('active');
      for (const p of document.querySelectorAll('.tab-panel')) p.classList.add('hidden');
      btn.classList.add('active');
      const tab = btn.getAttribute('data-tab');
      state.activeTab = tab;
      el(`tab-${tab}`).classList.remove('hidden');
    });
  }

  // Steer send
  async function sendSteer() {
    if (!state.selectedId) return;
    const input = el('steer-input');
    const msg = input.value.trim();
    if (!msg) return;
    const mode = el('steer-mode').value;
    input.value = '';
    showFeedback('Sending steer instruction…');
    const res = await apiPost('/api/subagent/steer', {
      conversationId: state.selectedId,
      message: msg,
      mode,
    });
    if (res.error) {
      showFeedback(`Error: ${res.error}`, true);
    } else {
      showFeedback(`Applied (${res.action || mode})`);
    }
    await pollAll();
  }

  el('btn-send-steer').addEventListener('click', sendSteer);
  el('steer-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendSteer();
    }
  });

  // Quick actions: Interrupt / Revert / Kill
  el('btn-interrupt').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const res = await apiPost('/api/subagent/interrupt', { conversationId: state.selectedId });
    showFeedback(res.error ? `Error: ${res.error}` : `Interrupted (${res.action})`);
    pollAll();
  });

  el('btn-revert-last').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const res = await apiPost('/api/subagent/revert', { conversationId: state.selectedId, stepOrTurns: 1 });
    showFeedback(res.error ? `Error: ${res.error}` : `Reverted (${res.action})`);
    pollAll();
  });

  el('btn-kill').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const res = await apiPost('/api/subagent/kill', { conversationId: state.selectedId });
    showFeedback(res.error ? `Error: ${res.error}` : 'Subagent terminated');
    pollAll();
  });

  // Approvals
  el('btn-approve-accept').addEventListener('click', async () => {
    if (!state.selectedId) return;
    await apiPost('/api/subagent/approve', { conversationId: state.selectedId, decision: 'accept' });
    showFeedback('Approved request');
    pollAll();
  });

  el('btn-approve-decline').addEventListener('click', async () => {
    if (!state.selectedId) return;
    await apiPost('/api/subagent/approve', { conversationId: state.selectedId, decision: 'decline' });
    showFeedback('Declined request');
    pollAll();
  });

  // Config & Compaction
  el('btn-apply-cfg').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const model = el('cfg-model').value.trim() || undefined;
    const effort = el('cfg-effort').value;
    const permissionSource = el('cfg-perm-source') ? el('cfg-perm-source').value : undefined;
    const sandboxMode = el('cfg-sandbox') ? el('cfg-sandbox').value : undefined;
    const approvalPolicy = el('cfg-approval').value;
    const res = await apiPost('/api/subagent/configure', {
      conversationId: state.selectedId,
      model,
      effort,
      permissionSource,
      sandboxMode,
      approvalPolicy,
    });
    showFeedback(
      res.error
        ? `Error: ${res.error}`
        : `Updated permissions (${res.permissionSource}: ${res.approvalPolicy}/${res.sandboxMode})`
    );
    pollAll();
  });

  if (el('btn-bypass-now')) {
    el('btn-bypass-now').addEventListener('click', async () => {
      if (!state.selectedId) return;
      const res = await apiPost('/api/subagent/configure', {
        conversationId: state.selectedId,
        permissionSource: 'bypass',
        approvalPolicy: 'never',
        sandboxMode: 'danger-full-access',
      });
      showFeedback(
        res.error
          ? `Error: ${res.error}`
          : '⚡ Bypass Permissions enabled (approvalPolicy=never, sandbox=danger-full-access)'
      );
      pollAll();
    });
  }

  // Adopt / Take Over Existing Codex Session Modal
  async function loadAdoptableThreads() {
    const sel = el('adopt-thread-select');
    if (!sel) return;
    sel.innerHTML = '<option value="">Loading Codex threads from daemon…</option>';
    try {
      const data = await apiGet('/api/codex/threads', { limit: '30' });
      const threads = data.threads || [];
      sel.replaceChildren(new Option('— Select a Codex session to take over —', ''));
      for (const t of threads) {
        const tag = t.isLoaded ? '🟢 [LOADED]' : '⚪ [DISK]';
        const adopted = t.isAdopted ? ' (already adopted)' : '';
        const prev = (t.preview || t.cwd || 'No preview').replace(/\s+/g, ' ').slice(0, 56);
        const label = `${tag} ${t.threadId.slice(0, 10)} · ${t.model || 'codex'} · ${prev}${adopted}`;
        sel.add(new Option(label, t.threadId));
      }
    } catch (e) {
      sel.replaceChildren(new Option(`Failed to load threads: ${e.message}`, ''));
    }
  }

  if (el('btn-adopt-modal')) {
    el('btn-adopt-modal').addEventListener('click', () => {
      el('adopt-modal').classList.remove('hidden');
      loadAdoptableThreads();
    });
  }
  if (el('btn-close-adopt-modal')) {
    el('btn-close-adopt-modal').addEventListener('click', () => {
      el('adopt-modal').classList.add('hidden');
    });
  }
  if (el('btn-refresh-threads')) {
    el('btn-refresh-threads').addEventListener('click', loadAdoptableThreads);
  }
  if (el('adopt-thread-select')) {
    el('adopt-thread-select').addEventListener('change', (e) => {
      if (e.target.value) {
        el('adopt-thread-id').value = e.target.value;
      }
    });
  }
  if (el('btn-confirm-adopt')) {
    el('btn-confirm-adopt').addEventListener('click', async () => {
      const threadId = (el('adopt-thread-id').value || el('adopt-thread-select').value || '').trim();
      if (!threadId) {
        showFeedback('Please select or enter a Codex Thread ID to take over', true);
        return;
      }
      const permissionSource = el('adopt-perm-source').value;
      const role = el('adopt-role').value.trim() || undefined;
      const prompt = el('adopt-prompt').value.trim() || undefined;
      el('adopt-modal').classList.add('hidden');
      showFeedback(`Taking over Codex session ${threadId.slice(0, 12)}…`);
      const res = await apiPost('/api/codex/adopt', {
        threadId,
        permissionSource,
        role,
        prompt,
      });
      if (res.error) {
        showFeedback(`Adopt failed: ${res.error}`, true);
      } else {
        state.selectedId = res.conversationId;
        showFeedback(`Took over Codex thread ${threadId.slice(0, 10)} (${res.hydratedTurns || 0} turns imported)`);
      }
      await pollAll();
    });
  }

  el('btn-compact-thread').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const res = await apiPost('/api/subagent/configure', {
      conversationId: state.selectedId,
      compact: true,
    });
    showFeedback(res.error ? `Error: ${res.error}` : 'Triggered thread/compact/start');
  });

  // Fork & Handoff
  el('btn-run-handoff').addEventListener('click', async () => {
    if (!state.selectedId) return;
    const target = el('handoff-target').value;
    const newRole = el('handoff-role').value.trim() || undefined;
    const prompt = el('handoff-prompt').value.trim() || undefined;

    let res;
    if (target === 'fork') {
      res = await apiPost('/api/subagent/fork', {
        conversationId: state.selectedId,
        newRole,
        prompt,
      });
    } else {
      res = await apiPost('/api/subagent/handoff', {
        sourceConversationId: state.selectedId,
        targetEngine: target,
        newRole,
        prompt: prompt || 'Continue from the previous subagent findings.',
      });
    }
    if (res.error) {
      showFeedback(`Error: ${res.error}`, true);
    } else {
      showFeedback(`Executed ${res.action}`);
      if (res.newConversationId) {
        state.selectedId = res.newConversationId;
      }
    }
    pollAll();
  });

  // Spawn Codex modal
  el('btn-spawn-modal').addEventListener('click', () => {
    el('spawn-modal').classList.remove('hidden');
  });
  el('btn-close-modal').addEventListener('click', () => {
    el('spawn-modal').classList.add('hidden');
  });
  el('btn-confirm-spawn').addEventListener('click', async () => {
    const role = el('spawn-role').value.trim() || 'Codex Specialist';
    const cwd = el('spawn-cwd').value.trim();
    const effort = el('spawn-effort').value;
    const prompt = el('spawn-prompt').value.trim();
    if (!prompt) return;
    el('spawn-modal').classList.add('hidden');
    const res = await apiPost('/api/subagent/spawn-codex', {
      role,
      cwd,
      effort,
      prompt,
    });
    if (res.conversationId) {
      state.selectedId = res.conversationId;
      showFeedback(`Spawned Codex thread ${res.threadId || ''}`);
    }
    pollAll();
  });

  pollAll();
  setInterval(pollAll, 2000);
});
