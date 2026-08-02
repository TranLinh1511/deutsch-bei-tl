// ==================== FIREBASE ====================
    import { initializeApp } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-app.js";
    import { getAuth, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-auth.js";
    import {
      getFirestore,
      collection,
      doc,
      setDoc,
      getDoc,
      getDocs,
      deleteDoc,
      writeBatch,
      query,
      orderBy,
      onSnapshot,
    } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-firestore.js";

    const _app = initializeApp({
      apiKey: "AIzaSyAF016vyl1zZ7dSTpmMZkj8BhCQVwmELl0",
      authDomain: "deutschbei-tl.firebaseapp.com",
      projectId: "deutschbei-tl",
      storageBucket: "deutschbei-tl.firebasestorage.app",
      messagingSenderId: "698311590550",
      appId: "1:698311590550:web:fd9006993fb023641138b7",
    });
    const _auth = getAuth(_app);
    const _db = getFirestore(_app);

    // Chờ Firebase Auth xác nhận trạng thái đăng nhập
    // (Firebase tự khôi phục session từ IndexedDB - không cần đăng nhập lại)
    let _uid = "default";
    let _authResolved = false;
    const _authReady = new Promise((resolve) => {
      onAuthStateChanged(_auth, (user) => {
        if (!_authResolved) {
          _authResolved = true;
          if (user) {
            _uid = user.uid;
            // Đồng bộ lại localStorage để debug helper vẫn hoạt động
            localStorage.setItem("loggedUid", user.uid);
            localStorage.setItem("loggedIn", "1");
            resolve(user);
          } else {
            // Không có Auth session → về trang login
            window.location.replace("login.html");
          }
        }
      });
    });

    const STATE_KEY = () => "germanAppState_v5_" + _uid;

    // ── Firestore path helpers ──
    const sessCol = () => collection(_db, "users", _uid, "sessions");
    const sessDoc = (sid) => doc(_db, "users", _uid, "sessions", sid);
    const vocabCol = (sid) =>
      collection(_db, "users", _uid, "sessions", sid, "vocabulary");
    const vocabDoc = (sid, wid) =>
      doc(_db, "users", _uid, "sessions", sid, "vocabulary", wid);
    const mastCol = (sid) =>
      collection(_db, "users", _uid, "sessions", sid, "mastered");
    const mastDoc = (sid, wid) =>
      doc(_db, "users", _uid, "sessions", sid, "mastered", wid);
    const flagCol = (sid) =>
      collection(_db, "users", _uid, "sessions", sid, "flagged");
    const flagDoc = (sid, wid) =>
      doc(_db, "users", _uid, "sessions", sid, "flagged", wid);
    const folderCol = () => collection(_db, "users", _uid, "folders");
    const folderDoc = (fid) => doc(_db, "users", _uid, "folders", fid);

    // ── In-memory cache ──
    const _cache = {
      sessions: null,
      vocab: {},    // sid → array
      mastered: {}, // sid → Set
      flagged: {},  // sid → Set
      folders: null,
    };

    // ── Real-time listener registry ──
    const _listeners = {}; // sid → { vocab, mastered, flagged } unsubscribe fns

    function _startSessionListeners(sid, onRemoteChange) {
      if (_listeners[sid]) return;
      _listeners[sid] = {};

      // Vocab — only react to server-confirmed changes (skip local writes)
      _listeners[sid].vocab = onSnapshot(
        query(vocabCol(sid), orderBy("sortOrder", "asc")),
        { includeMetadataChanges: true },
        (snap) => {
          // hasPendingWrites=true means this event is from our own local write
          // — cache is already up-to-date from the write path, skip re-render
          if (snap.metadata.hasPendingWrites) return;
          const data = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
          _cache.vocab[sid] = data;
          localStorage.setItem(`vocab_${sid}`, JSON.stringify(data));
          // fromCache=true on first load means offline cache — still update memory
          // but only trigger re-render if it's a real server update
          if (!snap.metadata.fromCache) onRemoteChange("vocab", sid);
        },
        (err) => console.warn("[onSnapshot vocab]", err)
      );

      // Mastered
      _listeners[sid].mastered = onSnapshot(
        mastCol(sid),
        { includeMetadataChanges: true },
        (snap) => {
          if (snap.metadata.hasPendingWrites) return;
          _cache.mastered[sid] = new Set(snap.docs.map((d) => d.id));
          localStorage.setItem("cache_mastered_" + sid, JSON.stringify([..._cache.mastered[sid]]));
          if (!snap.metadata.fromCache) onRemoteChange("mastered", sid);
        },
        (err) => console.warn("[onSnapshot mastered]", err)
      );

      // Flagged
      _listeners[sid].flagged = onSnapshot(
        flagCol(sid),
        { includeMetadataChanges: true },
        (snap) => {
          if (snap.metadata.hasPendingWrites) return;
          _cache.flagged[sid] = new Set(snap.docs.map((d) => d.id));
          localStorage.setItem("cache_flagged_" + sid, JSON.stringify([..._cache.flagged[sid]]));
          if (!snap.metadata.fromCache) onRemoteChange("flagged", sid);
        },
        (err) => console.warn("[onSnapshot flagged]", err)
      );
    }

    function _stopSessionListeners(sid) {
      if (!_listeners[sid]) return;
      Object.values(_listeners[sid]).forEach((unsub) => unsub());
      delete _listeners[sid];
    }

    function initRealtimeSync(sid, onRemoteChange) {
      _startSessionListeners(sid, onRemoteChange);
    }
    function switchRealtimeSession(newSid, onRemoteChange) {
      Object.keys(_listeners).forEach((sid) => {
        if (sid !== newSid) _stopSessionListeners(sid);
      });
      _startSessionListeners(newSid, onRemoteChange);
    }
    function addRealtimeSession(sid, onRemoteChange) {
      _startSessionListeners(sid, onRemoteChange);
    }

    // ==================== DB FUNCTIONS ====================
    async function dbGetAllSessions() {
      if (_cache.sessions) return _cache.sessions;
      try {
        const snap = await getDocs(
          query(sessCol(), orderBy("createdAt", "asc")),
        );
        _cache.sessions = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
        localStorage.setItem("cache_sessions_" + _uid, JSON.stringify(_cache.sessions));
        return _cache.sessions;
      } catch (err) {
        // Fallback to localStorage cache
        const cached = localStorage.getItem("cache_sessions_" + _uid);
        if (cached) {
          console.warn("Firestore unavailable, using local cache:", err?.code);
          _cache.sessions = JSON.parse(cached);
          return _cache.sessions;
        }
        throw err;
      }
    }
    async function dbSaveSession(s) {
      const data = { name: s.name, createdAt: s.createdAt || Date.now() };
      if (s.folderId !== undefined) data.folderId = s.folderId || null;
      await setDoc(sessDoc(s.id), data, { merge: true });
      _cache.sessions = null;
    }
    async function dbDeleteSession(sid) {
      const [v, m, f] = await Promise.all([
        getDocs(vocabCol(sid)),
        getDocs(mastCol(sid)),
        getDocs(flagCol(sid)),
      ]);
      const batch = writeBatch(_db);
      [...v.docs, ...m.docs, ...f.docs].forEach((d) => batch.delete(d.ref));
      batch.delete(sessDoc(sid));
      await batch.commit();
      _cache.sessions = null;
      delete _cache.vocab[sid];
      delete _cache.mastered[sid];
      delete _cache.flagged[sid];
    }
    async function dbGetSessionVocab(sid) {
      // In-memory cache is kept fresh by onSnapshot listener — use it if available
      if (_cache.vocab[sid]) return _cache.vocab[sid];
      // localStorage fallback (e.g. before listener fires on first load)
      const cached = localStorage.getItem(`vocab_${sid}`);
      if (cached) {
        _cache.vocab[sid] = JSON.parse(cached);
        return _cache.vocab[sid];
      }
      // Cold start: fetch from Firestore once, listener will keep it updated
      const snap = await getDocs(
        query(vocabCol(sid), orderBy("sortOrder", "asc")),
      );
      const result = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      _cache.vocab[sid] = result;
      localStorage.setItem(`vocab_${sid}`, JSON.stringify(result));
      return result;
    }
    function invalidateVocabCache(sid) {
      // No longer needed to clear — onSnapshot keeps cache fresh.
      // We still clear localStorage so other tabs/devices don't read stale data.
      localStorage.removeItem(`vocab_${sid}`);
      delete _cache.vocab[sid];
    }
    async function dbAddWord(sid, word, order) {
      await setDoc(vocabDoc(sid, word.id), {
        originalGerman: word.originalGerman,
        mainGerman: word.mainGerman,
        meaning: word.meaning,
        wordType: word.wordType || "",
        example: word.example || "",
        sortOrder: order || 0,
      });
      delete _cache.vocab[sid];
      invalidateVocabCache(sid);
    }
    async function dbUpdateWord(sid, id, g, main, m, wt, ex) {
      await setDoc(
        vocabDoc(sid, id),
        {
          originalGerman: g,
          mainGerman: main,
          meaning: m,
          wordType: wt || "",
          example: ex || "",
        },
        { merge: true },
      );
      delete _cache.vocab[sid];
      invalidateVocabCache(sid);
    }
    async function dbDeleteWord(sid, id) {
      await deleteDoc(vocabDoc(sid, id));
      delete _cache.vocab[sid];
      invalidateVocabCache(sid);
    }
    async function dbGetMastered(sid) {
      // Cache kept fresh by onSnapshot; use if available
      if (_cache.mastered[sid]) return _cache.mastered[sid];
      try {
        const snap = await getDocs(mastCol(sid));
        _cache.mastered[sid] = new Set(snap.docs.map((d) => d.id));
        localStorage.setItem("cache_mastered_" + sid, JSON.stringify([..._cache.mastered[sid]]));
        return _cache.mastered[sid];
      } catch (err) {
        const cached = localStorage.getItem("cache_mastered_" + sid);
        if (cached) {
          _cache.mastered[sid] = new Set(JSON.parse(cached));
          return _cache.mastered[sid];
        }
        _cache.mastered[sid] = new Set();
        return _cache.mastered[sid];
      }
    }
    async function dbMarkMastered(sid, wid) {
      await setDoc(mastDoc(sid, wid), { masteredAt: Date.now() });
      if (_cache.mastered[sid]) _cache.mastered[sid].add(wid);
    }
    async function dbUnmarkMastered(sid, wid) {
      await deleteDoc(mastDoc(sid, wid));
      if (_cache.mastered[sid]) _cache.mastered[sid].delete(wid);
    }
    async function dbGetFlagged(sid) {
      if (_cache.flagged[sid]) return _cache.flagged[sid];
      try {
        const snap = await getDocs(flagCol(sid));
        _cache.flagged[sid] = new Set(snap.docs.map((d) => d.id));
        localStorage.setItem("cache_flagged_" + sid, JSON.stringify([..._cache.flagged[sid]]));
        return _cache.flagged[sid];
      } catch (err) {
        const cached = localStorage.getItem("cache_flagged_" + sid);
        if (cached) {
          _cache.flagged[sid] = new Set(JSON.parse(cached));
          return _cache.flagged[sid];
        }
        _cache.flagged[sid] = new Set();
        return _cache.flagged[sid];
      }
    }
    async function dbMarkFlagged(sid, wid) {
      await setDoc(flagDoc(sid, wid), { flaggedAt: Date.now() });
      if (_cache.flagged[sid]) _cache.flagged[sid].add(wid);
    }
    async function dbUnmarkFlagged(sid, wid) {
      await deleteDoc(flagDoc(sid, wid));
      if (_cache.flagged[sid]) _cache.flagged[sid].delete(wid);
    }
    function updateModeFloatBar() {
      const mobMode = document.getElementById("mobModeSelect");
      if (mobMode) mobMode.value = exerciseMode;
    }
    // ==================== EXPORT/IMPORT ====================
    async function exportSessionsToJson(sessionIds) {
      const data = {
        _format: "germanApp_sessions_v1",
        _exported: new Date().toISOString(),
        sessions: [],
      };
      for (const sid of sessionIds) {
        const sessions = await dbGetAllSessions();
        const sessInfo = sessions.find((s) => s.id === sid);
        if (!sessInfo) continue;
        const [vocab, mIds, fIds] = await Promise.all([
          dbGetSessionVocab(sid),
          dbGetMastered(sid),
          dbGetFlagged(sid),
        ]);
        data.sessions.push({
          id: sessInfo.id,
          name: sessInfo.name,
          createdAt: sessInfo.createdAt,
          vocabulary: vocab.map((w) => ({
            id: w.id,
            originalGerman: w.originalGerman,
            mainGerman: w.mainGerman,
            meaning: w.meaning,
            wordType: w.wordType || "",
            example: w.example || "",
            mastered: mIds.has(w.id),
            flagged: fIds.has(w.id),
          })),
        });
      }
      return data;
    }
    async function mobileDownload(blob, filename, mimeType) {
      // 1. Try Web Share API (works in many Android WebView / PWA builders)
      if (navigator.share && navigator.canShare) {
        try {
          const file = new File([blob], filename, { type: mimeType });
          if (navigator.canShare({ files: [file] })) {
            await navigator.share({ files: [file], title: filename });
            return true;
          }
        } catch (e) { /* fall through */ }
      }
      // 2. Fallback: base64 data URI — avoids blob: URI which WebView APK builders can't handle
      return new Promise((resolve) => {
        const reader = new FileReader();
        reader.onload = () => {
          const a = document.createElement("a");
          a.href = reader.result; // data: URI
          a.download = filename;
          a.style.display = "none";
          document.body.appendChild(a);
          a.click();
          setTimeout(() => {
            try { document.body.removeChild(a); } catch (e) { }
          }, 600);
          resolve(true);
        };
        reader.onerror = () => resolve(false);
        reader.readAsDataURL(blob);
      });
    }
    function isMobileWebView() {
      // Detect APK-builder / WebView environments that can't handle blob: URIs
      const ua = navigator.userAgent || "";
      // "Website 2 APK Builder" wraps in a standard Android WebView
      return /wv|WebView|; wv\)/i.test(ua) ||
        /Android.*Version\/[\d.]+.*Chrome\/[\d.]+/i.test(ua) ||
        (window.Android != null) ||
        (window.ReactNativeWebView != null) ||
        (typeof window.AppBuilder !== 'undefined') ||
        // Extra: if we're in standalone / installed PWA on Android, also safer to use data URI
        (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches && /Android/i.test(ua));
    }
    async function downloadBlob(blob, filename, mimeType) {
      if (isMobileWebView()) {
        return mobileDownload(blob, filename, mimeType);
      }
      try {
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        a.style.display = "none";
        document.body.appendChild(a);
        a.click();
        setTimeout(() => {
          try { document.body.removeChild(a); } catch (e) { }
          URL.revokeObjectURL(url);
        }, 600);
      } catch (e) {
        // blob: URI failed (e.g. WebView that slipped through detection), fall back to data URI
        return mobileDownload(blob, filename, mimeType);
      }
    }
    async function importSessionsFromData(data) {
      if (!data || !Array.isArray(data.sessions))
        return { ok: false, msg: "File không hợp lệ!" };
      let totalSessions = 0,
        totalWords = 0;
      const allSessions = await dbGetAllSessions();
      for (const sess of data.sessions) {
        if (!sess.name || !Array.isArray(sess.vocabulary)) continue;
        const existing = allSessions.find((s) => s.name === sess.name);
        let sid;
        if (existing) {
          sid = existing.id;
        } else {
          sid =
            "sess_" +
            Date.now() +
            "_" +
            Math.random().toString(36).slice(2, 7);
          await dbSaveSession({
            id: sid,
            name: sess.name,
            createdAt: sess.createdAt || Date.now(),
          });
          totalSessions++;
        }
        const currentVocab = await dbGetSessionVocab(sid);
        const existingGerman = new Set(
          currentVocab.map((v) => v.originalGerman.toLowerCase()),
        );
        let wordOrder = currentVocab.length;
        for (const w of sess.vocabulary) {
          if (!w.originalGerman || !w.meaning) continue;
          if (existingGerman.has(w.originalGerman.toLowerCase())) continue;
          const newId = uid();
          await dbAddWord(
            sid,
            {
              id: newId,
              originalGerman: w.originalGerman,
              mainGerman:
                w.mainGerman || w.originalGerman.split("/")[0].trim(),
              meaning: w.meaning,
              wordType: w.wordType || "",
              example: w.example || "",
            },
            wordOrder++,
          );
          if (w.mastered) await dbMarkMastered(sid, newId);
          if (w.flagged) await dbMarkFlagged(sid, newId);
          totalWords++;
        }
      }
      return { ok: true, totalSessions, totalWords };
    }

    // ==================== STATE ====================
    let currentSessionId = null,
      currentSource = "session",
      currentExerciseType = "fullWord";
    let currentQIndex = 0,
      currentQuestionsList = [],
      stats = { totalAttempts: 0, correctCount: 0 };
    let randomMode = false,
      wordLimit = 0,
      autoAdvanceOnCorrect = true,
      soundEnabled = true;
    let isDarkMode = true,
      isWaitingForAutoNext = false,
      isCustomMode = false,
      studyMode = false;
    // Bật/tắt cho phép chọn (tích) phiên để gộp trong modal Quản lý thư mục
    let folderMergeSelectMode = false;
    // Cách sắp xếp danh sách phiên bên trong mỗi thư mục: "default" (theo thứ tự tạo) | "name" (theo tên A-Z)
    let folderSortMode = localStorage.getItem("folderSortMode") || "default";
    // exerciseMode: "write" | "choose" | "listen"
    let exerciseMode = "write",
      sidebarPage = 1,
      sidebarFilter = "",
      sidebarScope = "current",
      sidebarFilterTab = "all",
      sidebarTypeFilter = "all";
    let currentEditingWord = null,
      currentEditingSource = null;
    let mergedSessionIds = [],
      masteredIds = new Set(),
      flaggedIds = new Set(),
      allowSkip = false,
      onlyUnmastered = true,
      strictVocabCheck = false;
    const SIDEBAR_PER_PAGE = 15;

    // ==================== BREAK TIMER (nghỉ giải lao) ====================
    let breakEnabled = false,
      breakWorkMinutes = 25,
      breakRestMinutes = 5;
    let _breakWorkTimerId = null,
      _breakCountdownId = null,
      _breakRemainingSec = 0,
      _breakActive = false,
      _breakVideoEndedHandler = null;

    // ==================== CROSS-TAB SYNC ====================
    const _syncChannel = (() => {
      try {
        return new BroadcastChannel("deutschbei_sync_" + _uid);
      } catch (e) {
        return null;
      }
    })();

    // Gửi state sang tab khác mỗi khi save
    function _broadcastState(payload) {
      try {
        _syncChannel?.postMessage(payload);
      } catch (e) { }
    }

    // Nhận state từ tab khác
    if (_syncChannel) {
      _syncChannel.onmessage = async (ev) => {
        const d = ev.data;
        if (!d || d.type !== "stateUpdate") return;
        const s = d.state;
        // Cập nhật state hiện tại
        if (s.sessionId && s.sessionId !== currentSessionId) {
          currentSessionId = s.sessionId;
          _cache.sessions = null; // invalidate session cache
          masteredIds = await dbGetMastered(currentSessionId);
          flaggedIds = await dbGetFlagged(currentSessionId);
        }
        if (s.source !== undefined) currentSource = s.source;
        if (s.exerciseType !== undefined)
          currentExerciseType = s.exerciseType;
        if (s.exerciseMode !== undefined) exerciseMode = s.exerciseMode;
        if (s.wordLimit !== undefined) wordLimit = s.wordLimit;
        if (s.batchIdx !== undefined) window.batchIdx = s.batchIdx;
        if (s.mergedSessionIds) mergedSessionIds = s.mergedSessionIds;
        // Sync UI selects
        const sel = (id, v) => {
          const el = document.getElementById(id);
          if (el) el.value = v;
        };
        sel("sourceSelect", currentSource);
        sel("wordLimitSelect", String(wordLimit));
        sel("wordLimitSelectModal", String(wordLimit));
        sel("exerciseTypeSelect", currentExerciseType);
        sel("exerciseTypeSelectModal", currentExerciseType);
        // Session label updated via renderSessionDropdowns()
        _syncTypeSelectToMode(exerciseMode);
        updateAllToggles();
        // Nếu batch hoặc session thay đổi, reload danh sách
        await reloadPracticeList(false);
        await renderSidebar();
        await renderSessionDropdowns();
      };
    }

    function saveAppState() {
      try {
        const state = {
          sessionId: currentSessionId,
          source: currentSource,
          exerciseType: currentExerciseType,
          qIndex: currentQIndex,
          wordLimit,
          batchIdx: window.batchIdx || 0,
          isCustomMode,
          selectedWordIds: window.selectedWordIds || null,
          stats,
          sidebarFilterTab,
          sidebarScope,
          mergedSessionIds,
          exerciseMode,
          questionsList: currentQuestionsList.map((q) => ({
            id: q.id,
            isAnsweredCorrectly: q.isAnsweredCorrectly,
            _sessId: q._sessId,
          })),
        };
        localStorage.setItem(STATE_KEY(), JSON.stringify(state));
        // Phát sang các tab khác
        _broadcastState({ type: "stateUpdate", state });
      } catch (e) { }
    }
    function loadAppState() {
      try {
        const r = localStorage.getItem(STATE_KEY());
        return r ? JSON.parse(r) : null;
      } catch (e) {
        return null;
      }
    }

    // ==================== TTS ====================
    let _ttsUnlocked = false;
    let _silentAudio = null;

    function unlockTTS() {
      if (_ttsUnlocked) return;
      _ttsUnlocked = true;
      // Unlock AudioContext
      try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        ctx.resume();
      } catch (e) { }
      // Unlock Audio element for iOS: must play() inside user gesture
      try {
        // Shortest valid silent mp3 (base64)
        const silentSrc =
          "data:audio/mp3;base64,SUQzBAAAAAAAI1RTU0UAAAAPAAADTGF2ZjU4LjM0LjEwNAAAAAAAAAAAAAAA//OUQDvDUxFShoWWbHougyHjr0tFz3E38fX8e0bnTUpya-P0mXW///////////////////////////////////////////////////////////////////////////8AAAA8TEFNRTMuMTAwBK8AAAAAAAAAABSAJAJAQgAAgAAAA0L////////////////AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXALFSI6ETIZJH2N5CFT2CFOKPFDVDTZUVR7Q3L26UG74SWYGMY6X7MA46Q//MUZAAAAAGkAAAAAAAAA0gAAAAATEFN";
        _silentAudio = new Audio(silentSrc);
        _silentAudio.volume = 0;
        _silentAudio.play().catch(() => { });
      } catch (e) { }
      // Pre-load speech synthesis voices
      window.speechSynthesis?.getVoices();
    }

    let _ttsAudio = null;
    // ── Core TTS ──
    let _audioCtx = null;
    function _getAudioCtx() {
      if (!_audioCtx || _audioCtx.state === "closed") {
        _audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      }
      if (_audioCtx.state === "suspended") _audioCtx.resume();
      return _audioCtx;
    }

    // Phát qua Google TTS dùng fetch → AudioContext (tránh CORS của Audio element)
    function _speakGoogleFetch(text, langCode, onError) {
      const encoded = encodeURIComponent(text.trim());
      const url = `https://translate.googleapis.com/translate_tts?ie=UTF-8&q=${encoded}&tl=${langCode}&client=gtx&ttsspeed=0.8`;
      fetch(url)
        .then(r => { if (!r.ok) throw new Error("HTTP " + r.status); return r.arrayBuffer(); })
        .then(buf => {
          const ctx = _getAudioCtx();
          return ctx.decodeAudioData(buf);
        })
        .then(decoded => {
          const ctx = _getAudioCtx();
          if (_ttsSource) { try { _ttsSource.stop(); } catch (e) { } }
          _ttsSource = ctx.createBufferSource();
          _ttsSource.buffer = decoded;
          _ttsSource.connect(ctx.destination);
          _ttsSource.playbackRate.value = 0.92;
          _ttsSource.start(0);
        })
        .catch(err => {
          console.warn("Google TTS fetch error:", err);
          if (onError) onError();
        });
    }

    // Phát qua Audio element (cho mobile WebView — Google TTS hoạt động tốt)
    function _speakGoogleAudio(text, langCode, onError) {
      try {
        if (_ttsAudio) { _ttsAudio.pause(); _ttsAudio.src = ""; _ttsAudio = null; }
        const encoded = encodeURIComponent(text.trim());
        const url = `https://translate.googleapis.com/translate_tts?ie=UTF-8&q=${encoded}&tl=${langCode}&client=gtx&ttsspeed=0.8`;
        _ttsAudio = new Audio(url);
        _ttsAudio.volume = 1.0;
        const p = _ttsAudio.play();
        if (p) p.catch(err => { console.warn("TTS Audio error:", err); if (onError) onError(); });
      } catch (e) { console.warn("TTS Audio error:", e); if (onError) onError(); }
    }

    // Web Speech API fallback (chỉ hiệu quả khi có giọng đúng ngôn ngữ)
    function _speakWebSpeech(text, bcp47, langStart) {
      if (!window.speechSynthesis) return;
      window.speechSynthesis.cancel();
      const utt = new SpeechSynthesisUtterance(text.trim());
      utt.lang = bcp47;
      utt.rate = 0.85;
      utt.pitch = 1;
      const trySpeak = () => {
        const voices = window.speechSynthesis.getVoices();
        const voice = voices.find(v => v.lang.startsWith(langStart));
        if (voice) utt.voice = voice;
        window.speechSynthesis.speak(utt);
      };
      const voices = window.speechSynthesis.getVoices();
      if (voices.length > 0) trySpeak();
      else {
        window.speechSynthesis.onvoiceschanged = () => {
          window.speechSynthesis.onvoiceschanged = null; trySpeak();
        };
        setTimeout(() => { if (!window.speechSynthesis.speaking) trySpeak(); }, 300);
      }
    }

    function _speakLang(text, lang) {
      if (!soundEnabled || !text) return;
      const t = text.trim();
      const langCode = lang === "vi" ? "vi" : "de";
      const bcp47 = lang === "vi" ? "vi-VN" : "de-DE";
      const langStart = lang === "vi" ? "vi" : "de";
      const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);

      if (isMobile) {
        // Mobile WebView: Google TTS Audio element hoạt động tốt
        _speakGoogleAudio(t, langCode, () => _speakWebSpeech(t, bcp47, langStart));
      } else {
        // Desktop: thử Google TTS qua fetch+AudioContext (tránh CORS Audio element)
        // fallback về Web Speech nếu fetch lỗi
        _speakGoogleFetch(t, langCode, () => _speakWebSpeech(t, bcp47, langStart));
      }
    }

    // Phát tiếng Đức
    function speakText(text) { _speakLang(text, "de"); }

    // Phát âm tuỳ chế độ luyện tập
    function speakForMode(q) {
      if (!q) return;
      // Chế độ nhập câu: đọc câu ví dụ mẫu nếu có, không thì đọc từ gốc
      if (getEffectiveType(q) === "fullSentence") {
        speakText(q.example ? getGermanExample(q.example) : q.fullDisplayGerman);
        return;
      }
      // listen mode: LUÔN phát tiếng Đức (không phát tiếng Việt)
      // Khi rút gọn: đọc phần nguyên mẫu động từ (bỏ phần chia trong ngoặc)
      const reduced = getReducedTarget(q);
      speakText(reduced !== null ? reduced : q.fullDisplayGerman);
    }

    // ==================== UTILS ====================
    function uid() {
      return crypto.randomUUID
        ? crypto.randomUUID()
        : Date.now() + "-" + Math.random();
    }
    function escapeHtml(s) {
      const lookup = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#x27;",
      };
      return (s || "").replace(/[&<>"']/g, (m) => lookup[m]);
    }
    function shuffleArray(arr) {
      for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
      }
      return arr;
    }
    const _viMap = {
      á: "a", à: "a", ả: "a", ã: "a", ạ: "a", ă: "a", ắ: "a", ằ: "a", ẳ: "a", ẵ: "a", ặ: "a",
      â: "a", ấ: "a", ầ: "a", ẩ: "a", ẫ: "a", ậ: "a", đ: "d",
      é: "e", è: "e", ẻ: "e", ẽ: "e", ẹ: "e", ê: "e", ế: "e", ề: "e", ể: "e", ễ: "e", ệ: "e",
      í: "i", ì: "i", ỉ: "i", ĩ: "i", ị: "i",
      ó: "o", ò: "o", ỏ: "o", õ: "o", ọ: "o", ô: "o", ố: "o", ồ: "o", ổ: "o", ỗ: "o", ộ: "o",
      ơ: "o", ớ: "o", ờ: "o", ở: "o", ỡ: "o", ợ: "o",
      ú: "u", ù: "u", ủ: "u", ũ: "u", ụ: "u", ư: "u", ứ: "u", ừ: "u", ử: "u", ữ: "u", ự: "u",
      ý: "y", ỳ: "y", ỷ: "y", ỹ: "y", ỵ: "y",
    };
    function normalizeChar(c) { const l = c.toLowerCase(); return _viMap[l] || l; }
    // Chuẩn hoá chuỗi để tìm kiếm/so khớp: lowercase + bỏ dấu middot (·)
    function normSearch(s) { return (s || "").toLowerCase().replace(/·/g, ""); }
    function normalizeVi(s) {
      return s.toLowerCase().trim().split("").map(normalizeChar).join("")
        .replace(/[^\p{L}\p{N}\s]/gu, "").replace(/\s+/g, " ").trim();
    }
    function isMeaningMatch(u, c) {
      return normalizeVi(u) === normalizeVi(c);
    }
    function isMobileView() {
      return window.innerWidth <= 680;
    }
    function showToast(msg, dur = 1800) {
      const t = document.getElementById("toast");
      t.textContent = msg;
      t.style.opacity = "1";
      clearTimeout(t._timer);
      t._timer = setTimeout(() => (t.style.opacity = "0"), dur);
    }
    function normForCheck(s) {
      return s
        .replace(/[\/\<\>\(\)\,\-\[\]\+·]/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
    }
    // Normalize bỏ hoàn toàn dấu · (không thêm space) để khớp động từ ghép viết liền
    // VD: "mit·kommen" → "mitkommen", "an·rufen" → "anrufen"
    function normNoMiddot(s) {
      return s
        .replace(/·/g, "")
        .replace(/[\/\<\>\(\)\,\-\[\]\+]/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
    }
    // Khớp câu ví dụ: lấy phần tiếng Đức trước ngoặc dịch nghĩa (nếu có), bỏ dấu câu cuối, so sánh không phân biệt hoa/thường
    function normSentence(s) {
      return (s || "")
        .replace(/\(.*?\)\s*$/, "") // bỏ phần dịch nghĩa trong ngoặc ở cuối câu
        .trim()
        .replace(/[.!?]+$/, "") // bỏ dấu câu cuối
        .replace(/\s+/g, " ")
        .toLowerCase();
    }
    function isSentenceMatch(u, c) {
      if (!c) return false;
      return normSentence(u) === normSentence(c);
    }
    // Lấy phần tiếng Đức của câu ví dụ (bỏ phần dịch nghĩa tiếng Việt trong ngoặc ở cuối)
    function getGermanExample(example) {
      return (example || "").replace(/\(.*?\)\s*$/, "").trim();
    }
    function getVietnameseExample(example) {
      const m = (example || "").match(/\(([^()]*)\)\s*$/);
      return m ? m[1].trim() : "";
    }
    function isSmartMatch(u, c) {
      if (normForCheck(u) === normForCheck(c)) return true;
      // Chấp nhận viết liền động từ ghép: "mitkommen" khớp "mit·kommen"
      if (normNoMiddot(u) === normNoMiddot(c)) return true;
      return false;
    }
    // Khớp "nguyên từ" cho chế độ Nghe: phải nhập đúng toàn bộ từ
    function isOriginalWordMatch(u, c) {
      return isSmartMatch(u, c);
    }
    function parseGermanLine(raw) {
      const t = raw.trim();
      if (!t) return null;
      const sp = t.split(/\t+/);
      if (sp.length < 2) return null;
      const g = sp[0].trim();
      if (!g) return null;
      let wordType = "",
        meaning = "",
        example = "";
      if (sp.length >= 4) {
        wordType = sp[1].trim();
        meaning = sp[2].trim();
        example = sp.slice(3).join("\t").trim();
      } else if (sp.length === 3) {
        if (sp[1].trim().length <= 5) {
          wordType = sp[1].trim();
          meaning = sp[2].trim();
        } else {
          meaning = sp[1].trim();
          example = sp[2].trim();
        }
      } else {
        meaning = sp.slice(1).join(" ").trim();
      }
      if (!meaning) return null;
      let main = g.split("/")[0].trim();
      if (!main.match(/^(der|die|das)\s+\S+/)) main = g;
      return {
        originalGerman: g,
        mainGerman: main,
        meaning,
        wordType,
        example,
      };
    }
    function buildCharHint(userRaw, correctRaw) {
      const esc = (ch) =>
        ch === "<" ? "&lt;" : ch === ">" ? "&gt;" : ch === "&" ? "&amp;" : ch;

      // ── HINT ENGINE ──────────────────────────────────────────────────
      // Chiến lược: tách correctRaw thành 2 vùng để xử lý riêng:
      //   • Vùng CHÍNH (trước dấu ngoặc đầu tiên): so sánh char-by-char
      //     kể cả space — space thừa/thiếu trong từ ghép phải báo lỗi ngay.
      //   • Vùng PHỤ (phần ngoặc: conjugation, plural...): so sánh
      //     chỉ theo chữ+số+space, bỏ qua dấu câu (, ( ) ) để
      //     user không bị lỗi vì thiếu dấu phẩy hay ngoặc.
      // Dấu · trong correctRaw → bỏ khỏi so sánh và render (viết liền).
      // ─────────────────────────────────────────────────────────────────

      // Tách correctRaw thành phần chính và phần phụ (ngoặc)
      // VD: "funktionieren (du funktionierst, er funktioniert, funktionierte, hat funktioniert)"
      //   → main = "funktionieren ", paren = "(du funktionierst, er funktioniert, funktionierte, hat funktioniert)"
      // VD: "der Feuerwehrmann / die Feuerwehrmänner"
      //   → main = "der Feuerwehrmann / die Feuerwehrmänner", paren = ""
      const parenStart = correctRaw.indexOf("(");
      const mainRaw = parenStart >= 0 ? correctRaw.slice(0, parenStart) : correctRaw;
      const parenRaw = parenStart >= 0 ? correctRaw.slice(parenStart) : "";

      // Helper: strip middot
      const stripMiddot = (s) => s.replace(/·/g, "");

      // ── PHẦN CHÍNH: so sánh char-by-char ──
      // Ký tự đặc biệt (/, dấu câu) + space xung quanh chúng → skip khi so sánh, luôn render "ok"
      // User không cần gõ các ký tự này, hint vẫn tô xanh đúng

      // Strip ký tự đặc biệt + collapse spaces (áp dụng cho cả correct và user)
      function stripSpecialCollapse(s) {
        return s.replace(/\s*[\/\.\';!?,:\-()[\]{}]\s*/g, " ").replace(/\s+/g, " ").trim();
      }

      const mainCorrectRaw = stripMiddot(mainRaw).split("");
      // Chuẩn hóa user: strip special + collapse spaces
      const userStripped = stripSpecialCollapse(userRaw);
      const userFull = userStripped.split("").map(normalizeChar);

      // Correct cũng strip special để so sánh song song
      const correctStripped = stripSpecialCollapse(stripMiddot(mainRaw));
      const correctCmp = correctStripped.split("").map(normalizeChar);

      // charState cho correctCmp (chỉ chứa chữ+số+space thực sự cần gõ)
      const cmpState = []; // "ok" | "bad" | "pending"
      let hitBad = false;
      for (let i = 0; i < correctCmp.length; i++) {
        if (hitBad) { cmpState.push("pending"); continue; }
        if (i >= userFull.length) { cmpState.push("pending"); continue; }
        if (userFull[i] === correctCmp[i]) { cmpState.push("ok"); }
        else { cmpState.push("bad"); hitBad = true; }
      }

      // Map cmpState về mainCorrectRaw (gồm cả ký tự đặc biệt đã strip)
      // Ký tự đặc biệt → "skip"; space liền sau special → "skip"; còn lại → lấy từ cmpState
      const mainState = []; // "ok" | "bad" | "pending" | "skip"
      {
        let ci = 0;
        let prevSkipped = false;
        for (let i = 0; i < mainCorrectRaw.length; i++) {
          const ch = mainCorrectRaw[i];
          const isSpecial = /^[\/\.\';!?,:\-()[\]{}]$/.test(ch);
          if (isSpecial) {
            mainState.push("skip");
            prevSkipped = true;
            continue;
          }
          // Space sau ký tự đặc biệt → skip (đã collapse)
          if (ch === " " && prevSkipped) {
            mainState.push("skip");
            prevSkipped = false;
            continue;
          }
          prevSkipped = false;
          mainState.push(cmpState[ci] || "pending");
          ci++;
        }
      }

      // Con trỏ user sau phần chính
      const ui = Math.min(correctCmp.length, userFull.length);

      // ── PHẦN PHỤ (ngoặc): so sánh chỉ chữ+số+space, bỏ dấu câu ──
      // Lấy phần user còn lại sau các ký tự đã so sánh (ui đã trỏ đúng vị trí)
      const userRest = hitBad ? "" : userStripped.slice(ui);

      // Strip dấu câu (, . ( ) [ ]) khỏi cả correct và user trước khi so sánh
      const stripPunct = (s) => s.replace(/[().,\[\]{};:'"!?]/g, "").replace(/\s+/g, " ").trim();
      const parenCmp = stripMiddot(stripPunct(parenRaw)).split("").map(normalizeChar);
      const userRestCmp = stripPunct(userRest).split("").map(normalizeChar);

      // charState cho phần phụ — map về ký tự gốc của parenRaw (giữ dấu câu khi render)
      // Dùng alignment đơn giản: với mỗi char render trong parenRaw, tìm vị trí tương ứng trong parenCmp
      // Tính charState cho parenCmp trước
      const parenState = []; // "ok" | "bad" | "pending"
      for (let i = 0; i < parenCmp.length; i++) {
        if (hitBad) { parenState.push("pending"); continue; }
        if (i >= userRestCmp.length) { parenState.push("pending"); continue; }
        if (userRestCmp[i] === parenCmp[i]) { parenState.push("ok"); }
        else { parenState.push("bad"); hitBad = true; }
      }

      // ── RENDER ──
      let html = "";

      // Render phần chính (char-by-char, bỏ ·)
      let mi = 0;
      for (const ch of mainRaw) {
        if (ch === "·") continue;
        const state = mainState[mi] || "pending";
        mi++;
        if (state === "ok" || state === "skip") html += `<span class="ch-ok">${esc(ch)}</span>`;
        else if (state === "bad") html += `<span class="ch-bad">${esc(ch)}</span>`;
        else html += `<span>${esc(ch)}</span>`;
      }

      // Render phần phụ: duyệt parenRaw ký tự theo ký tự
      // Dấu câu (, . ( )) lấy state của char liền trước trong parenCmp
      if (parenRaw) {
        let pi = 0; // index trong parenCmp (chỉ tăng với char không phải punct)
        let lastParenState = hitBad && parenState.length === 0 ? "bad" : "pending";
        for (const ch of stripMiddot(parenRaw)) {
          const isPunct = /[().,\[\]{};:'"!?]/.test(ch);
          if (isPunct) {
            // Dùng state của ký tự liền trước
            const st = lastParenState;
            if (st === "ok") html += `<span class="ch-ok">${esc(ch)}</span>`;
            else if (st === "bad") html += `<span class="ch-bad">${esc(ch)}</span>`;
            else html += `<span>${esc(ch)}</span>`;
          } else {
            const state = parenState[pi] || "pending";
            lastParenState = state;
            pi++;
            if (state === "ok") html += `<span class="ch-ok">${esc(ch)}</span>`;
            else if (state === "bad") html += `<span class="ch-bad">${esc(ch)}</span>`;
            else html += `<span>${esc(ch)}</span>`;
          }
        }
      }

      return html;
    }

    function generateChoices(q, allList) {
      let correctAnswer, wrongPool;
      const effType = getEffectiveType(q);
      if (effType === "fullWord") {
        // listen: nghe DE → chọn nghĩa VI; write/choose: thấy VI → chọn từ DE
        if (exerciseMode === "listen") {
          correctAnswer = q.meaning;
          wrongPool = allList
            .filter((i) => i.id !== q.id)
            .map((i) => i.meaning);
        } else {
          correctAnswer = q.fullDisplayGerman;
          wrongPool = allList
            .filter((i) => i.id !== q.id)
            .map((i) => i.fullDisplayGerman);
        }
      } else if (effType === "fullMeaning") {
        // listen: nghe VI → chọn từ DE; write/choose: thấy DE → chọn nghĩa VI
        if (exerciseMode === "listen") {
          correctAnswer = q.fullDisplayGerman;
          wrongPool = allList
            .filter((i) => i.id !== q.id)
            .map((i) => i.fullDisplayGerman);
        } else {
          correctAnswer = q.meaning;
          wrongPool = allList
            .filter((i) => i.id !== q.id)
            .map((i) => i.meaning);
        }
      } else if (effType === "fullSentence") {
        // Chọn câu ví dụ đúng cho từ này, nhiễu là câu ví dụ của các từ khác
        correctAnswer = q.example;
        wrongPool = allList
          .filter((i) => i.id !== q.id && i.example)
          .map((i) => i.example);
      } else {
        correctAnswer = q.fullDisplayGerman;
        wrongPool = allList
          .filter((i) => i.id !== q.id)
          .map((i) => i.fullDisplayGerman);
      }
      const shuffled = shuffleArray([...new Set(wrongPool)]);
      return shuffleArray(
        [correctAnswer, ...shuffled.slice(0, 3)].slice(0, 4),
      ).map((c) => ({ text: c, isCorrect: c === correctAnswer }));
    }

    function openModal(id) {
      document.querySelectorAll(".modal-overlay.open").forEach((o) => {
        if (o.id !== id) o.classList.remove("open");
      });
      document.getElementById(id)?.classList.add("open");
    }
    function closeModal(id) {
      document.getElementById(id)?.classList.remove("open");
    }
    function loadSettings() {
      const ls = localStorage;
      isDarkMode = ls.getItem("darkMode") !== "false";
      randomMode = ls.getItem("randomMode") === "true";
      autoAdvanceOnCorrect = ls.getItem("autoAdvanceOnCorrect") !== "false";
      soundEnabled = ls.getItem("soundEnabled") !== "false";
      studyMode = ls.getItem("studyMode") === "true";
      exerciseMode = ls.getItem("exerciseMode") || "write";
      allowSkip = ls.getItem("allowSkip") !== "false";
      onlyUnmastered = ls.getItem("onlyUnmastered") !== "false";
      strictVocabCheck = ls.getItem("strictVocabCheck") === "true";
      breakEnabled = ls.getItem("breakEnabled") === "true";
      breakWorkMinutes = parseInt(ls.getItem("breakWorkMinutes"), 10) || 25;
      breakRestMinutes = parseInt(ls.getItem("breakRestMinutes"), 10) || 5;
    }
    function applyDark() {
      document.body.classList.remove("light");
      let dynStyle = document.getElementById("_lightOverride");
      if (isDarkMode === false) {
        document.body.classList.add("light");
        if (!dynStyle) {
          dynStyle = document.createElement("style");
          dynStyle.id = "_lightOverride";
          dynStyle.textContent = `
              body.light [class*="border-[#21262d]"] { border-color: var(--border) !important; }
              body.light [class*="border-[#30363d]"] { border-color: var(--border) !important; }
              body.light [class*="bg-[#0d1117]"] { background-color: var(--bg) !important; }
              body.light [class*="bg-[#161b22]"] { background-color: var(--bg2) !important; }
              body.light [class*="bg-[#1c2333]"] { background-color: var(--bg3) !important; }
              body.light [class*="text-[#e6edf3]"] { color: var(--tx) !important; }
              body.light [class*="text-[#8b949e]"] { color: var(--tx2) !important; }
              body.light [class*="text-[#6e7681]"] { color: var(--tx3) !important; }
              body.light .bg-\\[\\#1c2333\\] { background-color: var(--bg3) !important; }
            `;
          document.head.appendChild(dynStyle);
        }
      } else {
        dynStyle?.remove();
      }
    }
    function setToggle(id, val) {
      const b = document.getElementById(id);
      if (b) b.classList.toggle("on", val);
    }
    function updateAllToggles() {
      setToggle("tgDark", isDarkMode === true);
      setToggle("tgRandom", randomMode);
      setToggle("tgAutoAdv", autoAdvanceOnCorrect);
      setToggle("tgSpeak", soundEnabled);
      setToggle("tgStudy", studyMode);
      setToggle("tgFolderMergeMode", folderMergeSelectMode);
      setToggle("tgAllowSkip", allowSkip);
      setToggle("tgOnlyUnmastered", onlyUnmastered);
      setToggle("tgStrictVocab", strictVocabCheck);
      setToggle("tgBreakEnabled", breakEnabled);
      {
        const bwm = document.getElementById("breakWorkMinutes");
        if (bwm) bwm.value = breakWorkMinutes;
        const brm = document.getElementById("breakRestMinutes");
        if (brm) brm.value = breakRestMinutes;
      }

      // Label dạng bài
      const lbl = document.getElementById("exerciseModeLabel");
      if (lbl)
        lbl.textContent =
          exerciseMode === "choose"
            ? "Chọn"
            : exerciseMode === "listen"
              ? "Nghe"
              : "Viết";

      // 3 button mode trong settings
      ["tgModeWrite", "tgModeChoose", "tgModeListen"].forEach((btnId) => {
        const b = document.getElementById(btnId);
        if (!b) return;
        const isActive =
          (btnId === "tgModeWrite" && exerciseMode === "write") ||
          (btnId === "tgModeChoose" && exerciseMode === "choose") ||
          (btnId === "tgModeListen" && exerciseMode === "listen");
        b.style.borderColor = isActive ? "#58a6ff" : "";
        b.style.color = isActive ? "#58a6ff" : "";
        b.style.background = isActive ? "rgba(88,166,255,0.12)" : "";
      });

      // Modebar mode dropdown
      const modebarSel = document.getElementById("modebarModeSelect");
      if (modebarSel) modebarSel.value = exerciseMode;

      updateModeFloatBar();
    }

    // ==================== BREAK TIMER (nghỉ giải lao) ====================
    // Sau X phút làm việc, phủ video mèo lên trang, buộc nghỉ Y phút rồi tự tắt.
    // Video chạy hết Mèo 1 một lần, rồi chuyển sang Mèo 2 và lặp lại Mèo 2.
    function stopBreakWorkTimer() {
      if (_breakWorkTimerId) {
        clearTimeout(_breakWorkTimerId);
        _breakWorkTimerId = null;
      }
    }

    function startBreakWorkTimer() {
      stopBreakWorkTimer();
      if (!breakEnabled || _breakActive) return;
      const ms = Math.max(1, breakWorkMinutes) * 60 * 1000;
      _breakWorkTimerId = setTimeout(() => {
        showBreakOverlay();
      }, ms);
    }

    function _formatBreakTime(sec) {
      const m = Math.floor(sec / 60);
      const s = Math.floor(sec % 60)
        .toString()
        .padStart(2, "0");
      return `${m}:${s}`;
    }

    function _playBreakVideo(video, src, loop) {
      video.loop = loop;
      video.src = src;
      video.currentTime = 0;
      video.play().catch(() => { });
    }

    function showBreakOverlay() {
      const overlay = document.getElementById("breakOverlay");
      const video = document.getElementById("breakVideo");
      const countdownEl = document.getElementById("breakCountdown");
      if (!overlay || !video) return;
      _breakActive = true;
      overlay.style.display = "flex";
      document.body.style.overflow = "hidden";

      const isMobileBreak = window.matchMedia("(max-width: 680px)").matches;

      // Gỡ handler "ended" cũ (nếu có) trước khi gắn mới, tránh gọi chồng
      if (_breakVideoEndedHandler) {
        video.removeEventListener("ended", _breakVideoEndedHandler);
        _breakVideoEndedHandler = null;
      }

      if (!isMobileBreak) {
        // Reset trạng thái "ngủ", tắt animation cũ để chuẩn bị chạy lại từ đầu
        video.classList.remove("breakSleeping");
        video.pause();
        video.style.animation = "none";
        void video.offsetWidth; // force reflow
        video.loop = false;
        video.src = "assets/videos/neko1.webm";
        video.load();
        video.currentTime = 0;

        // Chờ lấy được thời lượng thật của video 1, rồi mới cho animation
        // trượt-vào-từ-phải chạy với duration = đúng thời lượng đó, để video
        // vừa trượt tới vị trí giữa (căn giữa màn hình) đúng lúc video 1 vừa hết.
        // Có timeout dự phòng: nếu vì lý do gì đó không lấy được metadata
        // (autoplay bị chặn, file lỗi, v.v.), animation vẫn chắc chắn được
        // kích hoạt sau tối đa 500ms, tránh bị "kẹt" đứng yên không trượt.
        let _slideStarted = false;
        const _startSlideIn = () => {
          if (_slideStarted) return;
          _slideStarted = true;
          const dur =
            video.duration && isFinite(video.duration) && video.duration > 0
              ? video.duration
              : 1.1;
          video.style.animation = "none";
          void video.offsetWidth; // force reflow để restart keyframe animation
          video.style.animation = `breakSlideIn ${dur}s cubic-bezier(0.22, 0.9, 0.3, 1) forwards`;
          video.play().catch(() => { });
        };
        video.addEventListener("loadedmetadata", _startSlideIn, { once: true });
        video.addEventListener("canplay", _startSlideIn, { once: true });
        setTimeout(_startSlideIn, 500);

        // Khi video 1 kết thúc → chuyển sang video 2, lặp lại, đứng yên (không trượt lại)
        _breakVideoEndedHandler = () => {
          video.classList.add("breakSleeping");
          _playBreakVideo(video, "assets/videos/neko2.webm", true);
        };
        video.addEventListener("ended", _breakVideoEndedHandler, { once: true });
      } else {
        video.pause();
        video.removeAttribute("src");
        video.load();
      }

      _breakRemainingSec = Math.max(1, breakRestMinutes) * 60;
      if (countdownEl) countdownEl.textContent = _formatBreakTime(_breakRemainingSec);

      clearInterval(_breakCountdownId);
      _breakCountdownId = setInterval(() => {
        _breakRemainingSec--;
        if (countdownEl)
          countdownEl.textContent = _formatBreakTime(Math.max(0, _breakRemainingSec));
        if (_breakRemainingSec <= 0) {
          hideBreakOverlay();
        }
      }, 1000);
    }

    function hideBreakOverlay() {
      const overlay = document.getElementById("breakOverlay");
      const video = document.getElementById("breakVideo");
      clearInterval(_breakCountdownId);
      _breakCountdownId = null;
      _breakActive = false;
      if (overlay) overlay.style.display = "none";
      if (video) {
        if (_breakVideoEndedHandler) {
          video.removeEventListener("ended", _breakVideoEndedHandler);
        }
        video.pause();
        video.removeAttribute("src");
        video.load();
      }
      _breakVideoEndedHandler = null;
      document.body.style.overflow = "";
      showToast?.("✅ Hết giờ nghỉ, tiếp tục học nào!", 2200);
      // Bắt đầu lại chu kỳ làm việc mới
      startBreakWorkTimer();
    }

    function restartBreakCycle() {
      // Gọi khi bật/tắt hoặc thay đổi thời lượng — không ảnh hưởng nếu đang trong giờ nghỉ
      stopBreakWorkTimer();
      if (breakEnabled && !_breakActive) startBreakWorkTimer();
    }


    // window.prompt() và window.confirm() KHÔNG hoạt động khi app chạy ở chế độ
    // "Thêm vào màn hình chính" (standalone PWA) trên iOS Safari — chúng bị chặn
    // hoàn toàn và im lặng không hiện gì cả. Thay bằng modal HTML tự dựng để
    // hoạt động ổn định trên mọi môi trường (kể cả PWA standalone).
    let _cpOverlay = null;
    function _ensureCustomPromptModal() {
      if (_cpOverlay) return _cpOverlay;
      _cpOverlay = document.createElement("div");
      _cpOverlay.id = "_customPromptOverlay";
      _cpOverlay.style.cssText =
        "display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:100000;align-items:center;justify-content:center;padding:16px;";
      _cpOverlay.innerHTML = `
        <div id="_cpBox" style="background:var(--modal-bg);border:1px solid var(--border);border-radius:12px;box-shadow:0 12px 40px rgba(0,0,0,.5);width:100%;max-width:340px;padding:18px;">
          <div id="_cpMessage" style="font-size:.9rem;color:var(--tx);margin-bottom:12px;white-space:pre-wrap;"></div>
          <input id="_cpInput" type="text" style="width:100%;box-sizing:border-box;background:var(--modal-input-bg);border:1px solid var(--input-border);color:var(--input-color);border-radius:8px;padding:9px 11px;font-size:.9rem;margin-bottom:14px;display:none;" />
          <div style="display:flex;gap:8px;justify-content:flex-end;">
            <button id="_cpCancelBtn" style="padding:8px 14px;border-radius:8px;border:1px solid var(--border);background:var(--btn-bg);color:var(--tx);font-size:.85rem;cursor:pointer;">Hủy</button>
            <button id="_cpOkBtn" style="padding:8px 14px;border-radius:8px;border:none;background:#58a6ff;color:#0d1117;font-weight:700;font-size:.85rem;cursor:pointer;">OK</button>
          </div>
        </div>`;
      document.body.appendChild(_cpOverlay);
      return _cpOverlay;
    }
    function customPrompt(message, defaultValue = "") {
      return new Promise((resolve) => {
        const overlay = _ensureCustomPromptModal();
        const msgEl = overlay.querySelector("#_cpMessage");
        const inputEl = overlay.querySelector("#_cpInput");
        const okBtn = overlay.querySelector("#_cpOkBtn");
        const cancelBtn = overlay.querySelector("#_cpCancelBtn");
        msgEl.textContent = message;
        inputEl.style.display = "block";
        inputEl.value = defaultValue || "";
        overlay.style.display = "flex";
        setTimeout(() => { inputEl.focus(); inputEl.select(); }, 30);
        const cleanup = (result) => {
          overlay.style.display = "none";
          okBtn.onclick = null;
          cancelBtn.onclick = null;
          inputEl.onkeydown = null;
          resolve(result);
        };
        okBtn.onclick = () => cleanup(inputEl.value);
        cancelBtn.onclick = () => cleanup(null);
        inputEl.onkeydown = (e) => {
          if (e.key === "Enter") { e.preventDefault(); cleanup(inputEl.value); }
          if (e.key === "Escape") { e.preventDefault(); cleanup(null); }
        };
      });
    }
    function customConfirm(message) {
      return new Promise((resolve) => {
        const overlay = _ensureCustomPromptModal();
        const msgEl = overlay.querySelector("#_cpMessage");
        const inputEl = overlay.querySelector("#_cpInput");
        const okBtn = overlay.querySelector("#_cpOkBtn");
        const cancelBtn = overlay.querySelector("#_cpCancelBtn");
        msgEl.textContent = message;
        inputEl.style.display = "none";
        okBtn.textContent = "Xác nhận";
        overlay.style.display = "flex";
        const cleanup = (result) => {
          overlay.style.display = "none";
          okBtn.textContent = "OK";
          okBtn.onclick = null;
          cancelBtn.onclick = null;
          resolve(result);
        };
        okBtn.onclick = () => cleanup(true);
        cancelBtn.onclick = () => cleanup(false);
      });
    }

    // ==================== FOLDER HELPERS ====================
    async function getFolders() {
      if (_cache.folders) return _cache.folders;
      try {
        const snap = await getDocs(query(folderCol(), orderBy("order")));
        const folders = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
        _cache.folders = folders;
        return folders;
      } catch {
        // fallback to localStorage for offline
        try {
          return JSON.parse(localStorage.getItem("sessionFolders") || "[]");
        } catch {
          return [];
        }
      }
    }
    async function createFolder(name, parentId = null) {
      const folders = await getFolders();
      const f = { id: "folder_" + Date.now(), name, order: folders.length, parentId: parentId || null };
      await setDoc(folderDoc(f.id), { name: f.name, order: f.order, parentId: f.parentId });
      _cache.folders = null; // invalidate cache
      return f;
    }
    async function renameFolder(fid, name) {
      await setDoc(folderDoc(fid), { name }, { merge: true });
      _cache.folders = null; // invalidate cache
    }
    async function deleteFolder(fid) {
      await deleteDoc(folderDoc(fid));
      _cache.folders = null; // invalidate cache
    }
    async function setSessionFolder(sessId, folderId) {
      const sessions = await dbGetAllSessions();
      const sess = sessions.find((s) => s.id === sessId);
      if (!sess) return;
      await dbSaveSession({ ...sess, folderId: folderId || null });
    }

    async function renderFolderModal() {
      const folders = await getFolders();
      const sessions = await dbGetAllSessions();
      const sessWithCount = await Promise.all(
        sessions.map(async (s) => ({
          ...s,
          count: (await dbGetSessionVocab(s.id)).length,
        })),
      );
      const list = document.getElementById("folderList");
      if (!list) return;

      // Giữ nguyên trạng thái mở/đóng thư mục qua các lần render lại
      // (chỉ khởi tạo lần đầu, không reset về collapsed mỗi khi render)
      if (!window._folderCollapsed) window._folderCollapsed = {};

      list.innerHTML = "";

      // Current session banner
      const curSess = sessWithCount.find(s => s.id === currentSessionId);
      if (curSess) {
        const banner = document.createElement("div");
        banner.style.cssText = "display:flex;align-items:center;gap:8px;padding:7px 12px;background:rgba(88,166,255,0.1);border:1px solid rgba(88,166,255,0.3);border-radius:8px;margin-bottom:8px;";
        banner.innerHTML = `<span style="font-size:.75rem;color:var(--tx3);flex-shrink:0">Đang học:</span>
          <span style="font-size:.83rem;font-weight:700;color:#58a6ff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1">${escapeHtml(curSess.name)}</span>
          <select class="mselect" id="wordLimitSelectModal" style="font-size:.72rem;padding:2px 6px;min-height:26px;flex-shrink:0" onchange="window.changeLimit(this.value)">
            <option value="0">Tất cả</option>
            <option value="5">5 từ</option>
            <option value="10">10 từ</option>
            <option value="15">15 từ</option>
            <option value="20">20 từ</option>
            <option value="30">30 từ</option>
            <option value="50">50 từ</option>
          </select>`;
        list.appendChild(banner);
        const _wlmBanner = document.getElementById("wordLimitSelectModal");
        if (_wlmBanner) _wlmBanner.value = String(wordLimit);
      }

      // Helper: sắp xếp danh sách phiên theo folderSortMode (giữ nguyên mảng gốc, trả về mảng mới)
      const sortSessList = (arr) =>
        folderSortMode === "name"
          ? [...arr].sort((a, b) =>
              a.name.localeCompare(b.name, "vi", { numeric: true, sensitivity: "base" }),
            )
          : arr;

      // Helper: render all sessions not in any valid folder as "uncategorized"
      const ungrouped = sortSessList(
        sessWithCount.filter(
          (s) => !s.folderId || !folders.find((f) => f.id === s.folderId),
        ),
      );

      const renderSessRow = (s, fid) => {
        const isActive = s.id === currentSessionId;
        const isMerged =
          fid &&
          currentSource === "merged" &&
          mergedSessionIds.includes(s.id);
        const div = document.createElement("div");
        div.className = "folder-sess-row";
        div.dataset.sessid = s.id;

        // Circle state: khi bật "Gộp phiên" → chỉ phản ánh đúng trạng thái đã tích chọn (isMerged),
        // KHÔNG tự động tích phiên hiện tại (phiên hiện tại đã được đánh dấu rõ qua khung màu của cả dòng).
        // Khi tắt "Gộp phiên" → giữ hành vi cũ: phiên hiện tại hiện dấu tick đặc để dễ nhận biết.
        const circleStyle = folderMergeSelectMode
          ? (isMerged
            ? `border:1.5px solid #58a6ff;background:rgba(88,166,255,.15);color:#58a6ff`
            : `border:1.5px solid var(--border2);background:transparent;color:transparent`)
          : (isActive
            ? `border:1.5px solid #58a6ff;background:#58a6ff;color:#fff`
            : isMerged
              ? `border:1.5px solid #58a6ff;background:rgba(88,166,255,.15);color:#58a6ff`
              : `border:1.5px solid var(--border2);background:transparent;color:transparent`);
        const circleContent = folderMergeSelectMode
          ? (isMerged ? '<i class="fa-solid fa-check"></i>' : "")
          : (isActive || isMerged ? '<i class="fa-solid fa-check"></i>' : "");

        const sessMenuId = "sess-menu-" + s.id;

        div.style.cssText = `display:flex;align-items:center;gap:6px;padding:5px 8px;border-radius:7px;margin-top:0;cursor:pointer;transition:background .12s,border-color .12s;border:1px solid ${isActive ? "rgba(88,166,255,.35)" : isMerged ? "rgba(88,166,255,.2)" : "transparent"};background:${isActive ? "rgba(88,166,255,.08)" : isMerged ? "rgba(88,166,255,.04)" : "var(--bg)"};position:relative;min-width:0;`;

        // Build move-to-folder options HTML
        const otherFolders = folders.filter(f => f.id !== fid);
        const moveOpts = otherFolders.length
          ? otherFolders.map(f => `<button class="sess-menu-item sess-move-folder" data-sessid="${s.id}" data-folderid="${f.id}" style="display:flex;align-items:center;gap:8px;width:100%;padding:7px 12px;background:none;border:none;color:var(--tx);font-size:.79rem;cursor:pointer;text-align:left" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><i class="fa-solid fa-folder-open"></i> → ${escapeHtml(f.name)}</button>`).join("")
          : `<div style="padding:7px 12px;font-size:.78rem;color:var(--tx3)">Chưa có thư mục nào</div>`;

        div.innerHTML = `
            <span class="sess-circle" style="width:16px;height:16px;border-radius:50%;${circleStyle};display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:.6rem;cursor:${fid && folderMergeSelectMode ? "pointer" : "default"};opacity:${fid && !folderMergeSelectMode ? 0.45 : 1}" title="${fid ? (folderMergeSelectMode ? "Tích để học gộp" : "Bật 'Gộp phiên' để chọn") : ""}">${circleContent}</span>
            <span class="sess-name-label" style="flex:1;font-size:.84rem;color:${isActive ? "var(--tx)" : "var(--tx2)"};font-weight:${isActive ? "700" : "400"};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;">${escapeHtml(s.name)}</span>
            <span style="font-size:.72rem;color:var(--tx3);flex-shrink:0">(${s.count})</span>
            <div style="position:relative;flex-shrink:0" onclick="event.stopPropagation()">
              <button class="sess-action-btn" data-sessid="${s.id}" data-menu="${sessMenuId}"
                style="font-size:.82rem;background:none;border:1px solid var(--border);color:var(--tx2);cursor:pointer;padding:1px 7px;border-radius:5px;line-height:1.7;transition:background .15s,color .15s,border-color .15s"
                title="Tùy chọn phiên">⋯</button>
              <div id="${sessMenuId}" style="display:none;position:absolute;right:0;top:calc(100% + 4px);background:var(--bg2);border:1px solid var(--border);border-radius:9px;box-shadow:0 6px 24px rgba(0,0,0,.4);min-width:185px;z-index:1000;overflow:hidden">
                <button class="sess-menu-item sess-rename-btn" data-sessid="${s.id}" data-sessname="${escapeHtml(s.name)}"
                  style="display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;background:none;border:none;color:var(--tx);font-size:.8rem;cursor:pointer;text-align:left"
                  onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'">Đổi tên</button>
                <div style="height:1px;background:var(--border);margin:0 8px"></div>
                <button class="sess-menu-item sess-export-json-btn" data-sessid="${s.id}" data-sessname="${escapeHtml(s.name)}"
                  style="display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;background:none;border:none;color:var(--tx);font-size:.8rem;cursor:pointer;text-align:left"
                  onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><i class="fa-solid fa-file-export"></i> Xuất JSON</button>
                <button class="sess-menu-item sess-export-excel-btn" data-sessid="${s.id}" data-sessname="${escapeHtml(s.name)}"
                  style="display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;background:none;border:none;color:var(--tx);font-size:.8rem;cursor:pointer;text-align:left"
                  onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><i class="fa-solid fa-file-excel"></i> Xuất Excel</button>
                <button class="sess-menu-item sess-delete-btn" data-sessid="${s.id}"
                  style="display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;background:none;border:none;color:#f78166;font-size:.8rem;cursor:pointer;text-align:left"
                  onmouseenter="this.style.background='rgba(247,129,102,.08)'" onmouseleave="this.style.background='none'"><i class="fa-solid fa-trash"></i> Xóa phiên</button>
                  <div style="height:1px;background:var(--border);margin:0 8px"></div>
                <button onclick="(function(btn){var body=btn.nextElementSibling;var arrow=btn.querySelector('.folder-arrow');var open=body.style.display==='none';body.style.display=open?'block':'none';arrow.textContent=open?'▴':'▾';})(this)" style="display:flex;align-items:center;justify-content:space-between;width:100%;padding:6px 12px 4px;background:none;border:none;cursor:pointer;font-size:.69rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--tx3)" onmouseenter="this.style.color='var(--tx2)'" onmouseleave="this.style.color='var(--tx3)'">Tuỳ chọn <span class="folder-arrow" style="font-size:.65rem">▾</span></button>
                <div style="display:none">
                ${moveOpts}
                ${fid ? `<div style="height:1px;background:var(--border);margin:0 8px"></div>
                <button class="sess-menu-item sess-remove-folder-btn" data-sessid="${s.id}"
                  style="display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;background:none;border:none;color:var(--tx3);font-size:.8rem;cursor:pointer;text-align:left"
                  onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><i class="fa-solid fa-xmark"></i> Xóa khỏi thư mục</button>` : ""}
                </div>
                <div style="height:1px;background:var(--border);margin:0 8px"></div>
                
              </div>
            </div>
          `;

        // Toggle chọn/huỷ chọn một phiên để gộp (click lần 1 = chọn, click lần 2 = huỷ chọn)
        const toggleMergeSelect = async (sid) => {
          const nowMerged = mergedSessionIds.includes(sid);
          if (nowMerged) {
            mergedSessionIds = mergedSessionIds.filter((id) => id !== sid);
          } else {
            if (!mergedSessionIds.includes(sid)) mergedSessionIds.push(sid);
          }
          if (mergedSessionIds.length > 0) {
            currentSource = "merged";
            document.getElementById("sourceSelect").value = "merged";
            const sModal = document.getElementById("sourceSelectModal");
            if (sModal) sModal.value = "merged";
          } else {
            currentSource = "session";
            document.getElementById("sourceSelect").value = "session";
            const sModal = document.getElementById("sourceSelectModal");
            if (sModal) sModal.value = "session";
          }
          window.customMaster = null;
          window.customFilterCriteria = null;
          window.selectedWordIds = null;
          window.batchIdx = 0;
          isCustomMode = false;
          saveAppState();
          showLoading("");
          try {
            await reloadPracticeList(true);
          } finally {
            hideLoading();
          }
          showToast(
            mergedSessionIds.length > 0
              ? `Gộp ${mergedSessionIds.length} phiên`
              : "📁 Phiên hiện tại",
          );
          await renderFolderModal();
        };

        // Click circle → toggle merge (chỉ khi bật "Gộp phiên" và phiên thuộc thư mục thật)
        const circle = div.querySelector(".sess-circle");
        if (fid && folderMergeSelectMode && circle) {
          circle.addEventListener("click", async (e) => {
            e.stopPropagation();
            await toggleMergeSelect(s.id);
          });
        }

        // ⋯ button → toggle dropdown
        const actionBtn = div.querySelector(".sess-action-btn");
        const menuEl = div.querySelector(`#${sessMenuId}`);
        const closeAllSessMenus = () => {
          list.querySelectorAll(".sess-action-btn").forEach(b => {
            const m = document.getElementById(b.dataset.menu);
            if (m) m.style.display = "none";
            b.style.background = "none";
            b.style.color = "var(--tx2)";
            b.style.borderColor = "var(--border)";
          });
        };
        actionBtn.addEventListener("click", (e) => {
          e.stopPropagation();
          const isOpen = menuEl.style.display !== "none";
          closeAllSessMenus();
          closeAllFolderMenus();
          if (!isOpen) {
            menuEl.style.display = "block";
            actionBtn.style.background = "var(--bg3)";
            actionBtn.style.color = "var(--tx)";
            actionBtn.style.borderColor = "#58a6ff";
          }
        });

        // Rename
        const renameBtn = div.querySelector(".sess-rename-btn");
        renameBtn.addEventListener("click", async (e) => {
          e.stopPropagation();
          closeAllSessMenus();
          const newName = await customPrompt("Đổi tên phiên:", s.name);
          if (!newName?.trim() || newName.trim() === s.name) return;
          const allSess = await dbGetAllSessions();
          const sess = allSess.find((x) => x.id === s.id);
          if (!sess) return;
          await dbSaveSession({ ...sess, name: newName.trim() });
          s.name = newName.trim();
          showToast("Đã đổi tên phiên");
          await renderSessionDropdowns();
          await renderFolderModal();
        });

        // 📤 Export session JSON
        const sessExportJsonBtn = div.querySelector(".sess-export-json-btn");
        if (sessExportJsonBtn) {
          sessExportJsonBtn.addEventListener("click", async (e) => {
            e.stopPropagation();
            closeAllSessMenus();
            showLoading("Đang xuất...");
            try {
              const data = await exportSessionsToJson([s.id]);
              const safeName = (s.name || "phien").replace(/[^a-zA-Z0-9_\-\u00C0-\u1EFF]/g, "_");
              downloadBlob(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }), `${safeName}.json`, "application/json");
              showToast(`✅ Đã xuất JSON: ${s.name}`);
            } finally { hideLoading(); }
          });
        }

        // 📊 Export session Excel
        const sessExportExcelBtn = div.querySelector(".sess-export-excel-btn");
        if (sessExportExcelBtn) {
          sessExportExcelBtn.addEventListener("click", async (e) => {
            e.stopPropagation();
            closeAllSessMenus();
            showLoading("Đang xuất...");
            try {
              const vocab = await dbGetSessionVocab(s.id);
              const safeName = (s.name || "phien").replace(/[^a-zA-Z0-9_\-\u00C0-\u1EFF]/g, "_");
              await exportToExcel(vocab, `${safeName}.xlsx`);
              showToast(`✅ Đã xuất Excel: ${s.name}`);
            } finally { hideLoading(); }
          });
        }

        // 📂 Move to folder
        div.querySelectorAll(".sess-move-folder").forEach(btn => {
          btn.addEventListener("click", async (e) => {
            e.stopPropagation();
            closeAllSessMenus();
            await setSessionFolder(btn.dataset.sessid, btn.dataset.folderid);
            showToast("📁 Đã chuyển vào thư mục");
            await renderFolderModal();
            await renderSessionDropdowns();
          });
        });

        // ✕ Remove from folder
        const removeFolderBtn = div.querySelector(".sess-remove-folder-btn");
        if (removeFolderBtn) {
          removeFolderBtn.addEventListener("click", async (e) => {
            e.stopPropagation();
            closeAllSessMenus();
            await setSessionFolder(s.id, null);
            showToast("✕ Đã xóa khỏi thư mục");
            await renderFolderModal();
            await renderSessionDropdowns();
          });
        }

        // 🗑️ Delete session
        const deleteBtn = div.querySelector(".sess-delete-btn");
        deleteBtn.addEventListener("click", async (e) => {
          e.stopPropagation();
          closeAllSessMenus();
          const allSessions = await dbGetAllSessions();
          if (allSessions.length <= 1) {
            showToast("⚠️ Không thể xóa phiên duy nhất!");
            return;
          }
          if (!(await customConfirm("Xóa hoàn toàn phiên này khỏi app? Không thể khôi phục!"))) return;
          showLoading("Đang xóa...");
          try {
            await dbDeleteSession(s.id);
            if (currentSessionId === s.id) {
              const remaining = await dbGetAllSessions();
              currentSessionId = remaining[0].id;
              masteredIds = await dbGetMastered(currentSessionId);
              flaggedIds = await dbGetFlagged(currentSessionId);
            }
            mergedSessionIds = mergedSessionIds.filter((id) => id !== s.id);
            window.customMaster = null;
            window.customFilterCriteria = null;
            window.selectedWordIds = null;
            window.batchIdx = 0;
            isCustomMode = false;
            await reloadPracticeList(true);
            await renderFolderModal();
            await renderSessionDropdowns();
            showToast("🗑️ Đã xóa phiên");
          } finally {
            hideLoading();
          }
        });

        // Click row → khi đang bật "Gộp phiên" thì chọn/huỷ chọn phiên để gộp;
        // ngược lại thì chuyển sang phiên đó như bình thường
        div.addEventListener("click", async (e) => {
          if (e.target.closest(".sess-action-btn") || e.target.closest(`#${sessMenuId}`)) return;
          if (e.target.classList.contains("sess-circle")) return;
          closeAllSessMenus();
          if (fid && folderMergeSelectMode) {
            await toggleMergeSelect(s.id);
            return;
          }
          await switchSession(s.id);
          closeModal("folderModal");
        });
        div.addEventListener("mouseenter", () => {
          if (!isActive)
            div.style.background = isMerged
              ? "rgba(88,166,255,.08)"
              : "var(--bg3)";
        });
        div.addEventListener("mouseleave", () => {
          if (!isActive)
            div.style.background = isMerged
              ? "rgba(88,166,255,.04)"
              : "var(--bg)";
        });
        return div;
      };

      // Count all sessions recursively (including sub-folders)
      const countAllInFolder = (fid) => {
        const direct = sessWithCount.filter(s => s.folderId === fid).length;
        const childFolderCount = folders.filter(f => f.parentId === fid)
          .reduce((sum, cf) => sum + countAllInFolder(cf.id), 0);
        return direct + childFolderCount;
      };
      const countSubFolders = (fid) => folders.filter(f => f.parentId === fid).length;

      const renderFolder = (fid, folderName, inFolder, opts = {}) => {
        const { isVirtual = false } = opts;
        const hasCurrent = inFolder.some((s) => s.id === currentSessionId);
        // Default collapsed; user must click to open
        const collapsed = window._folderCollapsed[fid] !== false;

        const card = document.createElement("div");

        // Header row
        const hdr = document.createElement("div");
        hdr.style.cssText = `display:flex;align-items:center;gap:6px;padding:9px 12px;background:${hasCurrent ? "rgba(88,166,255,0.08)" : "var(--bg2)"};cursor:pointer;user-select:none;border-radius:9px 9px 0 0;`;
        const menuId = "folder-menu-" + fid;
        const subActionsHtml = !isVirtual ? `
              <div style="position:relative;flex-shrink:0" onclick="event.stopPropagation()">
                <button class="folder-menu-btn" data-fid="${fid}" data-menu="${menuId}"
                  style="font-size:.85rem;background:none;border:1px solid var(--border);color:var(--tx2);cursor:pointer;padding:1px 8px;border-radius:5px;line-height:1.6;transition:background .15s,color .15s"
                  title="Tùy chọn">⋯</button>
                <!-- portal menu rendered dynamically -->
              </div>
            ` : "";
        hdr.innerHTML = `
            <span style="font-size:.9rem">${isVirtual ? '<i class="fa-solid fa-file"></i>' : '<i class="fa-solid fa-folder"></i>'}</span>
            <span style="flex:1;font-size:.88rem;font-weight:700;color:var(--tx);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;">${escapeHtml(folderName)}</span>
            <span style="font-size:.72rem;color:var(--tx3);flex-shrink:0;display:flex;gap:5px;align-items:center">
              <span title="Tổng phiên (bao gồm thư mục con)">${isVirtual ? inFolder.length : countAllInFolder(fid)} phiên</span>
              ${!isVirtual && countSubFolders(fid) > 0 ? `<span style="color:var(--tx3);opacity:.6">·</span><span title="Số thư mục con">${countSubFolders(fid)} <i class="fa-solid fa-folder"></i></span>` : ""}
            </span>
            ${subActionsHtml}
            <span class="folder-chev" style="font-size:.75rem;color:var(--tx3);flex-shrink:0;transition:transform .2s;transform:${collapsed ? "rotate(-90deg)" : "rotate(0deg)"}">${collapsed ? "▸" : "▾"}</span>
          `;

        const body = document.createElement("div");
        body.style.cssText = `padding:6px 8px 8px;background:${hasCurrent ? "rgba(88,166,255,0.04)" : "var(--bg2)"};display:${collapsed ? "none" : "grid"};grid-template-columns:1fr 1fr;gap:4px;border-radius:0 0 9px 9px;`;

        // Add blue left border accent to the card if contains current session
        card.style.cssText = `border:1px solid ${hasCurrent ? "rgba(88,166,255,0.35)" : "var(--border)"};border-radius:10px;overflow:visible;`;

        inFolder.forEach((s) =>
          body.appendChild(renderSessRow(s, isVirtual ? null : fid)),
        );

        // Render child sub-folders inside this folder
        if (!isVirtual) {
          const childFolders = folders.filter((f) => f.parentId === fid);
          if (childFolders.length) {
            const subFolderWrap = document.createElement("div");
            subFolderWrap.style.cssText = "grid-column:1/-1;margin-top:6px;padding-left:10px;border-left:2px solid rgba(88,166,255,0.2);display:flex;flex-direction:column;gap:5px;";
            childFolders.forEach((cf) => {
              const sessInChild = sortSessList(sessWithCount.filter((s) => s.folderId === cf.id));
              const childCard = renderFolder(cf.id, cf.name, sessInChild);
              subFolderWrap.appendChild(childCard);
            });
            body.appendChild(subFolderWrap);
          }
        }

        // "Add session" handled via shared modal (openAddSessModal)


        card.appendChild(hdr);
        card.appendChild(body);

        // Bottom divider line
        const divider = document.createElement("div");
        divider.style.cssText =
          "height:1px;background:var(--border);margin:0 12px;";
        card.appendChild(divider);

        // Toggle collapse — đóng các folder anh em khi mở folder này
        hdr.addEventListener("click", (e) => {
          if (e.target.tagName === "INPUT" || e.target.tagName === "BUTTON")
            return;
          const nowOpen = body.style.display !== "none";
          if (nowOpen) {
            // Đang mở → đóng lại
            body.style.display = "none";
            window._folderCollapsed[fid] = true;
            const chev = hdr.querySelector(".folder-chev");
            if (chev) { chev.textContent = "▸"; chev.style.transform = "rotate(-90deg)"; }
          } else {
            // Đang đóng → mở ra, đồng thời đóng các folder anh em
            const parentContainer = card.parentElement;
            if (parentContainer) {
              parentContainer.querySelectorAll(":scope > div").forEach((sibling) => {
                if (sibling === card) return;
                const sibBody = sibling.querySelector(":scope > div:nth-child(2)");
                const sibHdr = sibling.querySelector(":scope > div:nth-child(1)");
                const sibChev = sibHdr?.querySelector(".folder-chev");
                // Chỉ đóng nếu đang mở
                if (sibBody && sibBody.style.display !== "none") {
                  sibBody.style.display = "none";
                  if (sibChev) { sibChev.textContent = "▸"; sibChev.style.transform = "rotate(-90deg)"; }
                  // Lưu trạng thái collapsed cho sibling — lấy fid từ menu button
                  const sibFidEl = sibHdr?.querySelector("[data-fid]");
                  if (sibFidEl) window._folderCollapsed[sibFidEl.dataset.fid] = true;
                }
              });
            }
            body.style.display = "grid";
            window._folderCollapsed[fid] = false;
            const chev = hdr.querySelector(".folder-chev");
            if (chev) { chev.textContent = "▾"; chev.style.transform = "rotate(0deg)"; }
          }
        });

        return card;
      };

      // Render ungrouped (if any sessions not in a folder)
      if (ungrouped.length) {
        const ug = renderFolder(
          "__ungrouped__",
          "Chưa phân loại",
          ungrouped,
          { isVirtual: true },
        );
        list.appendChild(ug);
      }

      // Render real folders (root level only; sub-folders rendered recursively inside parents)
      folders.filter((f) => !f.parentId).forEach((f) => {
        const inFolder = sortSessList(sessWithCount.filter((s) => s.folderId === f.id));
        const card = renderFolder(f.id, f.name, inFolder);
        list.appendChild(card);
      });

      if (!folders.length && !ungrouped.length) {
        list.innerHTML = `<div style="color:var(--tx3);font-size:.8rem;text-align:center;padding:12px">Chưa có phiên nào.</div>`;
        return;
      }

      // Bind events
      // Close any open folder menu when clicking outside
      const closeAllFolderMenus = () => {
        const pm = document.getElementById("_folderPortalMenu");
        if (pm) pm.style.display = "none";
        list.querySelectorAll(".folder-menu-btn").forEach((b) => {
          b.style.background = "none";
          b.style.color = "var(--tx2)";
        });
        list.querySelectorAll(".sess-action-btn").forEach((b) => {
          const m = document.getElementById(b.dataset.menu);
          if (m) m.style.display = "none";
          b.style.background = "none";
          b.style.color = "var(--tx2)";
          b.style.borderColor = "var(--border)";
        });
      };
      // Portal dropdown — rendered to body to escape stacking context
      const FOLDER_MENU_BTNS_STYLE = `display:flex;align-items:center;gap:9px;width:100%;padding:7px 14px;background:none;border:none;font-size:.82rem;cursor:pointer;text-align:left;white-space:nowrap;transition:background .12s`;
      const folderMenuHtml = (fid, folderName) => `
        <div style="padding:4px 12px 6px;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--tx3)">Phiên</div>
        <button class="folder-new-sess-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:#3fb950" onmouseenter="this.style.background='rgba(63,185,80,.08)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-sparkles"></i></span>Tạo phiên mới</button>
        <button class="folder-menu-add-sess-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:var(--tx)" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-plus"></i></span>Thêm phiên có sẵn</button>
        <div style="height:1px;background:var(--border2);margin:4px 0"></div>
        <div style="padding:4px 12px 6px;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--tx3)">Thư mục</div>
        <button class="folder-add-sub-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:var(--tx)" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-folder"></i></span>Tạo thư mục con</button>
        <button class="folder-rename-btn" data-fid="${fid}" data-foldername="${escapeHtml(folderName)}" style="${FOLDER_MENU_BTNS_STYLE};color:var(--tx)" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"></span>Đổi tên thư mục</button>
        <div style="height:1px;background:var(--border2);margin:4px 0"></div>
        <div style="padding:4px 12px 6px;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--tx3)">Xuất</div>
        <button class="folder-export-json-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:var(--tx)" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-file-export"></i></span>Xuất JSON</button>
        <button class="folder-export-excel-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:var(--tx)" onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-file-excel"></i></span>Xuất Excel</button>
        <div style="height:1px;background:var(--border2);margin:4px 0"></div>
        <button class="folder-delete-btn" data-fid="${fid}" style="${FOLDER_MENU_BTNS_STYLE};color:#f78166" onmouseenter="this.style.background='rgba(247,129,102,.08)'" onmouseleave="this.style.background='none'"><span style="width:16px;text-align:center"><i class="fa-solid fa-trash"></i></span>Xóa thư mục</button>
        <div style="height:4px"></div>`;

      // ── Real handlers for folder actions (called directly from the portal menu) ──
      const doAddSubFolder = async (parentFid) => {
        const name = await customPrompt("Tên thư mục con mới:");
        if (!name?.trim()) return;
        const newSub = await createFolder(name.trim(), parentFid);
        if (!window._folderCollapsed) window._folderCollapsed = {};
        window._folderCollapsed[parentFid] = false; // keep parent expanded
        window._folderCollapsed[newSub.id] = false; // expand new sub-folder
        await renderFolderModal();
        await renderSessionDropdowns();
      };
      const doRenameFolder = async (fid, currentName) => {
        const newName = await customPrompt("Đổi tên thư mục:", currentName);
        if (!newName?.trim() || newName.trim() === currentName) return;
        await renameFolder(fid, newName.trim());
        await renderFolderModal();
        await renderSessionDropdowns();
      };
      const doExportFolderJson = async (fid) => {
        const sessInFolder = sessWithCount.filter((s) => s.folderId === fid);
        if (!sessInFolder.length) { showToast("⚠️ Thư mục trống!"); return; }
        showLoading("Đang xuất...");
        try {
          const folder = folders.find(f => f.id === fid);
          const safeName = (folder?.name || "thu-muc").replace(/[^a-zA-Z0-9_\-\u00C0-\u1EFF]/g, "_");
          const data = await exportSessionsToJson(sessInFolder.map(s => s.id));
          downloadBlob(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }), `${safeName}.json`, "application/json");
          showToast(`✅ Đã xuất ${sessInFolder.length} phiên`);
        } finally { hideLoading(); }
      };
      const doExportFolderExcel = async (fid) => {
        const sessInFolder = sessWithCount.filter((s) => s.folderId === fid);
        if (!sessInFolder.length) { showToast("⚠️ Thư mục trống!"); return; }
        showLoading("Đang xuất...");
        try {
          const folder = folders.find(f => f.id === fid);
          const safeName = (folder?.name || "thu-muc").replace(/[^a-zA-Z0-9_\-\u00C0-\u1EFF]/g, "_");
          let allWords = [];
          for (const s of sessInFolder) {
            const vocab = await dbGetSessionVocab(s.id);
            allWords = allWords.concat(vocab);
          }
          await exportToExcel(allWords, `${safeName}.xlsx`);
          showToast(`✅ Đã xuất Excel: ${folder?.name || ""}`);
        } finally { hideLoading(); }
      };
      const doDeleteFolder = async (fid) => {
        const allFolders = await getFolders();
        const childFolders = allFolders.filter((f) => f.parentId === fid);
        const msg = childFolders.length
          ? `Xóa thư mục và ${childFolders.length} thư mục con? Các phiên bên trong vẫn giữ nguyên.`
          : "Xóa thư mục? Các phiên bên trong vẫn giữ nguyên.";
        if (!(await customConfirm(msg))) return;
        // Remove sessions from this folder and all child folders
        const allFids = [fid, ...childFolders.map((f) => f.id)];
        const inAll = sessWithCount.filter((s) => allFids.includes(s.folderId));
        await Promise.all(inAll.map((s) => setSessionFolder(s.id, null)));
        await Promise.all(allFids.map((id) => deleteFolder(id)));
        await renderFolderModal();
        await renderSessionDropdowns();
      };

      let _portalMenu = document.getElementById("_folderPortalMenu");
      if (!_portalMenu) {
        _portalMenu = document.createElement("div");
        _portalMenu.id = "_folderPortalMenu";
        _portalMenu.style.cssText = "display:none;position:fixed;background:var(--bg2);border:1px solid var(--border);border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.55);min-width:210px;z-index:9999;overflow:hidden;padding:4px 0";
        document.body.appendChild(_portalMenu);
      }
      let _portalFid = null;

      const closePortalMenu = () => {
        _portalMenu.style.display = "none";
        _portalFid = null;
        list.querySelectorAll(".folder-menu-btn").forEach((b) => {
          b.style.background = "none";
          b.style.color = "var(--tx2)";
        });
        list.querySelectorAll(".sess-action-btn").forEach((b) => {
          b.style.background = "none";
          b.style.color = "var(--tx2)";
          b.style.borderColor = "var(--border)";
        });
      };

      if (window._folderMenuListener) document.removeEventListener("click", window._folderMenuListener);
      window._folderMenuListener = (e) => {
        if (!_portalMenu.contains(e.target) && !e.target.classList.contains("folder-menu-btn")) closePortalMenu();
      };
      document.addEventListener("click", window._folderMenuListener);

      list.querySelectorAll(".folder-menu-btn").forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const fid = btn.dataset.fid;
          const folderName = btn.closest("[data-folderid]")?.dataset?.folderid || "";
          if (_portalFid === fid) { closePortalMenu(); return; }
          closePortalMenu();
          _portalFid = fid;
          // Get folder name directly from the folders array (fetched at top of renderFolderModal)
          const fname = folders.find((f) => f.id === fid)?.name || "";
          _portalMenu.innerHTML = folderMenuHtml(fid, fname);
          // Position
          const rect = btn.getBoundingClientRect();
          const menuW = 220;
          let left = rect.right - menuW;
          if (left < 8) left = 8;
          let top = rect.bottom + 4;
          if (top + 380 > window.innerHeight) top = rect.top - 380;
          if (top < 8) top = 8;
          _portalMenu.style.left = left + "px";
          _portalMenu.style.top = top + "px";
          _portalMenu.style.display = "block";
          btn.style.background = "var(--bg3)";
          btn.style.color = "var(--tx)";
          // Wire up portal menu buttons — call handlers directly (these buttons only exist in portal, not in list)
          // ✨ Tạo phiên mới
          _portalMenu.querySelector(".folder-new-sess-btn")?.addEventListener("click", async (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            const targetFolder = folders.find((f) => f.id === fid);
            const name = await customPrompt(`Tên phiên mới trong "${targetFolder?.name || ""}":`);
            if (!name?.trim()) return;
            const ns = { id: "sess_" + Date.now(), name: name.trim(), createdAt: Date.now(), folderId: fid };
            showLoading("Đang tạo...");
            try {
              await dbSaveSession(ns);
            } finally { hideLoading(); }
            await renderFolderModal();
            await renderSessionDropdowns();
            showToast(`✅ Đã tạo phiên "${name.trim()}"`);
          });
          // ➕ Thêm phiên có sẵn
          _portalMenu.querySelector(".folder-menu-add-sess-btn")?.addEventListener("click", async (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            const targetFolder = folders.find((f) => f.id === fid);
            const parentId = targetFolder?.parentId ?? null;
            const available = sessWithCount.filter((s) => {
              if (s.folderId === fid) return false;
              if (!s.folderId) return true;
              if (s.folderId === parentId) return true;
              return false;
            });
            const title = document.getElementById("addSessModalTitle");
            if (title) title.innerHTML = `<i class="fa-solid fa-plus"></i> Thêm phiên vào "${escapeHtml(targetFolder?.name || "")}"`;
            const listEl = document.getElementById("addSessModalList");
            if (listEl) {
              if (available.length === 0) {
                listEl.innerHTML = `<div style="padding:14px;text-align:center;font-size:.82rem;color:var(--tx3)">Không còn phiên nào để thêm.</div>`;
              } else {
                listEl.innerHTML = available.map((s) => `
                  <label style="display:flex;align-items:center;gap:10px;padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--border2);transition:background .12s"
                    onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='transparent'">
                    <input type="checkbox" class="add-sess-cb" data-sessid="${s.id}" style="accent-color:#58a6ff;width:15px;height:15px;flex-shrink:0;cursor:pointer">
                    <span style="flex:1;font-size:.85rem;color:var(--tx)">${escapeHtml(s.name)}</span>
                    <span style="font-size:.72rem;color:var(--tx3)">(${s.count})</span>
                  </label>
                `).join("");
              }
            }
            document.getElementById("confirmAddSessModal").dataset.fid = fid;
            openModal("addSessModal");
          });
          // 📁 Tạo thư mục con
          _portalMenu.querySelector(".folder-add-sub-btn")?.addEventListener("click", (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            doAddSubFolder(fid);
          });
          // ✎ Đổi tên thư mục
          _portalMenu.querySelector(".folder-rename-btn")?.addEventListener("click", (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            doRenameFolder(fid, fname);
          });
          // 📤 Xuất JSON
          _portalMenu.querySelector(".folder-export-json-btn")?.addEventListener("click", (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            doExportFolderJson(fid);
          });
          // 📊 Xuất Excel
          _portalMenu.querySelector(".folder-export-excel-btn")?.addEventListener("click", (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            doExportFolderExcel(fid);
          });
          // 🗑️ Xóa thư mục
          _portalMenu.querySelector(".folder-delete-btn")?.addEventListener("click", (pe) => {
            pe.stopPropagation();
            closePortalMenu();
            doDeleteFolder(fid);
          });
        });
      });
      // (Tạo thư mục con / Đổi tên / Xuất JSON / Xuất Excel giờ được xử lý trực tiếp
      // qua doAddSubFolder / doRenameFolder / doExportFolderJson / doExportFolderExcel
      // ở phần wiring của portal menu phía trên — các nút này chỉ tồn tại trong
      // _portalMenu chứ không tồn tại trong `list`, nên gắn listener vào `list` là vô nghĩa.)
      // "Tất cả" / "Bỏ" quick-select merge buttons
      list.querySelectorAll(".folder-merge-all-btn").forEach((btn) => {
        btn.addEventListener("click", async (e) => {
          e.stopPropagation();
          const fid = btn.dataset.fid;
          const sessInFolder = sessWithCount.filter(
            (s) => s.folderId === fid,
          );
          sessInFolder.forEach((s) => {
            if (!mergedSessionIds.includes(s.id)) mergedSessionIds.push(s.id);
          });
          currentSource = "merged";
          document.getElementById("sourceSelect").value = "merged";
          const sModal = document.getElementById("sourceSelectModal");
          if (sModal) sModal.value = "merged";
          window.customMaster = null;
          window.customFilterCriteria = null;
          window.selectedWordIds = null;
          window.batchIdx = 0;
          isCustomMode = false;
          saveAppState();
          showLoading("");
          try {
            await reloadPracticeList(true);
          } finally {
            hideLoading();
          }
          showToast(`Gộp ${mergedSessionIds.length} phiên`);
          await renderFolderModal();
        });
      });
      list.querySelectorAll(".folder-merge-none-btn").forEach((btn) => {
        btn.addEventListener("click", async (e) => {
          e.stopPropagation();
          const fid = btn.dataset.fid;
          const sessInFolder = sessWithCount
            .filter((s) => s.folderId === fid)
            .map((s) => s.id);
          mergedSessionIds = mergedSessionIds.filter(
            (id) => !sessInFolder.includes(id),
          );
          if (mergedSessionIds.length === 0) {
            currentSource = "session";
            document.getElementById("sourceSelect").value = "session";
            const sModal = document.getElementById("sourceSelectModal");
            if (sModal) sModal.value = "session";
          }
          window.customMaster = null;
          window.customFilterCriteria = null;
          window.selectedWordIds = null;
          window.batchIdx = 0;
          isCustomMode = false;
          saveAppState();
          showLoading("");
          try {
            await reloadPracticeList(true);
          } finally {
            hideLoading();
          }
          showToast(
            mergedSessionIds.length > 0
              ? `Gộp ${mergedSessionIds.length} phiên`
              : "📁 Phiên hiện tại",
          );
          await renderFolderModal();
        });
      });

      // (Xóa thư mục giờ được xử lý trực tiếp qua doDeleteFolder ở phần wiring portal menu)
      // (Tạo phiên mới trong thư mục cũng xử lý trực tiếp trong wiring portal menu ở trên)
      // folder-delete-sess and folder-remove-sess are now handled inline in renderSessRow
      list.querySelectorAll(".folder-menu-add-sess-btn").forEach((btn) => {
        btn.addEventListener("click", async (e) => {
          e.stopPropagation();
          closeAllFolderMenus();
          const fid = btn.dataset.fid;
          const targetFolder = folders.find((f) => f.id === fid);
          const parentId = targetFolder?.parentId ?? null;
          const available = sessWithCount.filter((s) => {
            if (s.folderId === fid) return false;
            if (!s.folderId) return true;
            if (s.folderId === parentId) return true;
            return false;
          });
          // Populate modal
          const title = document.getElementById("addSessModalTitle");
          if (title) title.innerHTML = `<i class="fa-solid fa-plus"></i> Thêm phiên vào "${escapeHtml(targetFolder?.name || "")}"`;
          const listEl = document.getElementById("addSessModalList");
          if (listEl) {
            if (available.length === 0) {
              listEl.innerHTML = `<div style="padding:14px;text-align:center;font-size:.82rem;color:var(--tx3)">Không còn phiên nào để thêm.</div>`;
            } else {
              listEl.innerHTML = available.map((s) => `
                <label style="display:flex;align-items:center;gap:10px;padding:8px 12px;cursor:pointer;border-bottom:1px solid var(--border2);transition:background .12s"
                  onmouseenter="this.style.background='var(--bg3)'" onmouseleave="this.style.background='transparent'">
                  <input type="checkbox" class="add-sess-cb" data-sessid="${s.id}" style="accent-color:#58a6ff;width:15px;height:15px;flex-shrink:0;cursor:pointer">
                  <span style="flex:1;font-size:.85rem;color:var(--tx)">${escapeHtml(s.name)}</span>
                  <span style="font-size:.72rem;color:var(--tx3)">(${s.count})</span>
                </label>
              `).join("");
            }
          }
          // Store target fid for confirm
          document.getElementById("confirmAddSessModal").dataset.fid = fid;
          openModal("addSessModal");
        });
      });
    }

    async function renderSessionDropdowns() {
      const sessions = await dbGetAllSessions();
      const sessWithCount = await Promise.all(
        sessions.map(async (s) => {
          const vocab = await dbGetSessionVocab(s.id);
          return { ...s, count: vocab.length };
        }),
      );

      // Update current session label in header + settings
      const cur = sessWithCount.find((s) => s.id === currentSessionId);
      const curName = cur ? cur.name : "—";
      const hdrLabel = document.getElementById("currentSessionLabel");
      if (hdrLabel) hdrLabel.textContent = curName;

      // Re-render folder modal if open (to update active state)
      if (
        document.getElementById("folderModal")?.classList.contains("open")
      ) {
        await renderFolderModal();
      }
    }

    async function buildFullList(includeMastered = !onlyUnmastered) {
      if (currentSource === "merged") {
        const sessIds = mergedSessionIds.length
          ? mergedSessionIds
          : [currentSessionId];
        let combined = [];
        for (const sid of sessIds) {
          const vocab = await dbGetSessionVocab(sid),
            mIds = await dbGetMastered(sid),
            fIds = await dbGetFlagged(sid);
          vocab.forEach((item) => {
            if (!includeMastered && mIds.has(item.id)) return;
            combined.push({
              id: sid + ":" + item.id,
              _realId: item.id,
              _sessId: sid,
              fullDisplayGerman: item.originalGerman,
              mainGerman: item.mainGerman || item.originalGerman,
              meaning: item.meaning,
              wordType: item.wordType || "",
              example: item.example || "",
              isAnsweredCorrectly: false,
              isMastered: mIds.has(item.id),
              isFlagged: fIds.has(item.id),
            });
          });
        }
        return combined;
      }
      if (currentSource === "session") {
        const vocab = await dbGetSessionVocab(currentSessionId);
        return vocab
          .filter((item) => includeMastered || !masteredIds.has(item.id))
          .map((item) => ({
            id: item.id,
            _sessId: currentSessionId,
            fullDisplayGerman: item.originalGerman,
            mainGerman: item.mainGerman || item.originalGerman,
            meaning: item.meaning,
            wordType: item.wordType || "",
            example: item.example || "",
            isAnsweredCorrectly: false,
            isMastered: masteredIds.has(item.id),
            isFlagged: flaggedIds.has(item.id),
          }));
      }
      return [];
    }
    async function buildFullListAll() {
      // Lấy TẤT CẢ từ (kể cả đã thuộc), tôn trọng chế độ gộp phiên
      try {
        if (currentSource === "merged") {
          const sessIds = mergedSessionIds.length ? mergedSessionIds : [currentSessionId];
          let combined = [];
          for (const sid of sessIds) {
            const vocab = await dbGetSessionVocab(sid);
            const mIds = await dbGetMastered(sid);
            const fIds = await dbGetFlagged(sid);
            const isCurrentSess = sid === currentSessionId;
            vocab.forEach((item) => {
              combined.push({
                id: sid + ":" + item.id,
                _realId: item.id,
                _sessId: sid,
                _isOtherSess: !isCurrentSess,
                fullDisplayGerman: item.originalGerman,
                mainGerman: item.mainGerman || item.originalGerman,
                meaning: item.meaning,
                wordType: item.wordType || "",
                example: item.example || "",
                isAnsweredCorrectly: false,
                isMastered: mIds.has(item.id),
                isFlagged: fIds.has(item.id),
              });
            });
          }
          return combined;
        }
        // Phiên đơn
        const vocab = await dbGetSessionVocab(currentSessionId);
        const mIds = masteredIds instanceof Set ? masteredIds : await dbGetMastered(currentSessionId);
        const fIds = flaggedIds instanceof Set ? flaggedIds : await dbGetFlagged(currentSessionId);
        return vocab.map((item) => ({
          id: item.id,
          _sessId: currentSessionId,
          fullDisplayGerman: item.originalGerman,
          mainGerman: item.mainGerman || item.originalGerman,
          meaning: item.meaning,
          wordType: item.wordType || "",
          example: item.example || "",
          isAnsweredCorrectly: false,
          isMastered: mIds.has(item.id),
          isFlagged: fIds.has(item.id),
        }));
      } catch (err) {
        console.error("[buildFullListAll]", err);
        return [];
      }
    }

    // ── Loading overlay ──
    function showLoading(msg = "Đang tải...") {
      let el = document.getElementById("_fbLoading");
      if (!el) {
        el = document.createElement("div");
        el.id = "_fbLoading";
        el.style.cssText =
          "position:fixed;inset:0;background:rgba(13,17,23,.85);display:flex;align-items:center;justify-content:center;z-index:9999;font-family:DM Mono,monospace;font-size:.9rem;color:#58a6ff;gap:8px;";
        document.body.appendChild(el);
      }
      el.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> ${msg}`;
      el.style.display = "flex";
    }
    function hideLoading() {
      const el = document.getElementById("_fbLoading");
      if (el) el.style.display = "none";
    }

    async function reloadPracticeList(
      resetStats = false,
      restoreSnapshot = null,
    ) {
      showLoading("Đang tải từ vựng...");
      try {
        const fullList = await buildFullList();
        // Chỉ init customMaster khi đang ở chế độ phiên đơn và chưa có selection
        if (!window.customMaster && currentSource !== "merged") {
          window.customMaster = [...fullList];
          window.selectedWordIds = fullList.map((i) => i.id);
        }
        let sourceList =
          isCustomMode && window.customMaster?.length
            ? window.customMaster
            : fullList;
        let list;
        if (resetStats) window.batchIdx = 0;
        if (restoreSnapshot && restoreSnapshot.length) {
          const byId = new Map(fullList.map((i) => [i.id, i]));
          const resolved = restoreSnapshot
            .map((snap) => {
              // Thử match trực tiếp (merged: "sessId:wordId")
              let item = byId.get(snap.id);
              // Fallback: nếu merged và snapshot chưa có prefix, tìm theo _realId
              if (!item && snap._sessId) {
                item = byId.get(snap._sessId + ":" + snap.id);
              }
              if (!item) return null;
              return { ...item, isAnsweredCorrectly: snap.isAnsweredCorrectly };
            })
            .filter(Boolean);
          // Nếu snapshot không match được (đổi source), load fresh
          list = resolved.length ? resolved : [...sourceList];
          if (!resolved.length && wordLimit > 0 && list.length > wordLimit) {
            const start = (window.batchIdx || 0) * wordLimit;
            list = list.slice(start, Math.min(start + wordLimit, list.length));
          }
          // Khôi phục lại _shuffledOrder từ snapshot khi đang bật random,
          // nếu không thì lần gọi reloadPracticeList tiếp theo (không có restoreSnapshot,
          // ví dụ khi chuyển batch/gắn cờ) sẽ không thấy _shuffledOrder và tạo thứ tự
          // random MỚI hoàn toàn → nhìn như bị reset về đầu.
          if (randomMode && resolved.length) {
            const restoredIds = list.map((i) => i.id);
            const restoredSet = new Set(restoredIds);
            const remainingIds = sourceList
              .map((i) => i.id)
              .filter((id) => !restoredSet.has(id));
            window._shuffledOrder = [...restoredIds, ...remainingIds];
          }
        } else {
          if (randomMode && !restoreSnapshot) {
            const sourceIds = new Set(sourceList.map((i) => i.id));
            if (!window._shuffledOrder || window._shuffledOrder.length === 0) {
              // Chưa có thứ tự → tạo mới
              window._shuffledOrder = shuffleArray([...sourceList].map((i) => i.id));
            } else if (window._shuffledOrder.some((id) => !sourceIds.has(id))) {
              // Có ID không còn trong sourceList (từ đã thuộc) → lọc bớt, giữ thứ tự
              window._shuffledOrder = window._shuffledOrder.filter((id) => sourceIds.has(id));
            } else if (window._shuffledOrder.length < sourceList.length) {
              // Có từ mới chưa có trong order → thêm vào cuối
              const existingIds = new Set(window._shuffledOrder);
              const newIds = sourceList.map((i) => i.id).filter((id) => !existingIds.has(id));
              window._shuffledOrder = [...window._shuffledOrder, ...newIds];
            }
            const orderMap = new Map(
              window._shuffledOrder.map((id, idx) => [id, idx]),
            );
            list = [...sourceList].sort(
              (a, b) =>
                (orderMap.get(a.id) ?? 999) - (orderMap.get(b.id) ?? 999),
            );
          } else {
            if (!randomMode) window._shuffledOrder = null;
            list = [...sourceList];
          }
          if (wordLimit > 0 && list.length > wordLimit) {
            const start = (window.batchIdx || 0) * wordLimit;
            list = list.slice(
              start,
              Math.min(start + wordLimit, list.length),
            );
          }
        }
        currentQuestionsList = list;
        if (!restoreSnapshot)
          currentQuestionsList.forEach(
            (i) => (i.isAnsweredCorrectly = false),
          );
        currentQIndex = 0;
        isWaitingForAutoNext = false;
        if (resetStats) stats = { totalAttempts: 0, correctCount: 0 };
        renderExercise();
        await renderSidebar();
        updateBatchBar(sourceList);
        saveAppState();
        if (exerciseMode === "listen" && currentQuestionsList[0])
          speakForMode(currentQuestionsList[0]);
      } finally {
        hideLoading();
      }
    }

    // Cập nhật danh sách câu hỏi tại chỗ khi vocab thay đổi (sửa/thêm/xoá từ),
    // KHÔNG xáo trộn thứ tự và KHÔNG reset về câu đầu tiên như reloadPracticeList.
    // Lưu lại nội dung + vị trí con trỏ đang gõ trong ô trả lời (nếu đang ở đúng câu hỏi
    // hiện tại), để khôi phục lại sau khi renderExercise() bị gọi ngầm bởi một hành động
    // khác (đánh dấu chú ý, đánh dấu thuộc...) mà KHÔNG phải do người dùng chủ động
    // chuyển sang câu hỏi khác — tránh cảm giác input bị mất chữ như "bị reload lại".
    function _snapshotAnswerInput() {
      const inp = document.getElementById("dynamicAnswerInput");
      if (!inp || !inp.value) return null;
      const total = currentQuestionsList.length;
      const q = total ? currentQuestionsList[currentQIndex % total] : null;
      return {
        qId: q?.id,
        value: inp.value,
        selStart: inp.selectionStart,
        selEnd: inp.selectionEnd,
        focused: document.activeElement === inp,
      };
    }
    function _restoreAnswerInput(snap) {
      if (!snap) return;
      const total = currentQuestionsList.length;
      const q = total ? currentQuestionsList[currentQIndex % total] : null;
      // Câu hỏi đã đổi (chuyển câu, đổi batch...) → không khôi phục, tránh dán nhầm chữ
      if (!q || q.id !== snap.qId) return;
      const inp = document.getElementById("dynamicAnswerInput");
      if (!inp) return;
      inp.value = snap.value;
      if (snap.focused) {
        inp.focus({ preventScroll: true });
        try {
          inp.setSelectionRange(snap.selStart, snap.selEnd);
        } catch (e) { }
      }
    }
    async function mergeVocabChangesInPlace() {
      try {
        // Lưu lại nội dung đang gõ dở trong ô trả lời trước khi render lại,
        // để không bị mất chữ (cảm giác như bị reload) khi bấm chú ý/bỏ chú ý...
        const _inputSnap = _snapshotAnswerInput();
        const fresh = await buildFullList();
        const freshMap = new Map(fresh.map((i) => [i.id, i]));
        let removedBeforeCurrent = 0;
        const newList = [];
        currentQuestionsList.forEach((item, idx) => {
          const f = freshMap.get(item.id);
          if (!f) {
            // Từ đã bị xoá (hoặc không còn thoả điều kiện lọc) — loại khỏi danh sách
            if (idx < currentQIndex) removedBeforeCurrent++;
            return;
          }
          // Cập nhật dữ liệu mới nhất, giữ nguyên trạng thái làm bài hiện tại (isAnsweredCorrectly)
          item.fullDisplayGerman = f.fullDisplayGerman;
          item.mainGerman = f.mainGerman;
          item.meaning = f.meaning;
          item.wordType = f.wordType;
          item.example = f.example;
          item.isMastered = f.isMastered;
          item.isFlagged = f.isFlagged;
          newList.push(item);
          freshMap.delete(item.id);
        });
        // Từ mới thêm (chưa có trong danh sách hiện tại) — nối vào cuối, giữ nguyên thứ tự cũ,
        // nhưng chỉ nối thêm khi còn "chỗ trống" trong giới hạn wordLimit của batch hiện tại —
        // tránh làm patch/batch phình to vượt quá giới hạn đã đặt.
        if (wordLimit > 0) {
          let room = Math.max(0, wordLimit - newList.length);
          for (const f of freshMap.values()) {
            if (room <= 0) break;
            newList.push(f);
            room--;
          }
        } else {
          freshMap.forEach((f) => newList.push(f));
        }
        currentQuestionsList = newList;
        if (currentQuestionsList.length === 0) currentQIndex = 0;
        else {
          currentQIndex = Math.max(0, currentQIndex - removedBeforeCurrent);
          if (currentQIndex >= currentQuestionsList.length)
            currentQIndex = currentQuestionsList.length - 1;
        }
        // Đồng bộ customMaster (nếu đang dùng) với dữ liệu mới, không đổi thứ tự
        if (window.customMaster) {
          const custMap = new Map(fresh.map((i) => [i.id, i]));
          window.customMaster = window.customMaster
            .map((item) => {
              const f = custMap.get(item.id);
              if (!f) return null;
              item.fullDisplayGerman = f.fullDisplayGerman;
              item.mainGerman = f.mainGerman;
              item.meaning = f.meaning;
              item.wordType = f.wordType;
              item.example = f.example;
              item.isMastered = f.isMastered;
              item.isFlagged = f.isFlagged;
              return item;
            })
            .filter(Boolean);
        }
        renderExercise();
        _restoreAnswerInput(_inputSnap);
        saveAppState();
        await syncCustomSelectionWithFilter();
      } catch (err) {
        console.error("[mergeVocabChangesInPlace]", err);
      }
    }

    async function renderSidebar() {
      try { return await _renderSidebarInner(); }
      catch (e) { console.error("[renderSidebar]", e); }
    }
    async function _renderSidebarInner() {
      const isSearchingAllSessions = sidebarScope === "all";
      const isSearching = sidebarFilter.trim().length > 0;
      let allWords,
        sessionsAll = null;

      if (isSearchingAllSessions) {
        // Hiển thị / tìm trong TẤT CẢ phiên
        sessionsAll = await dbGetAllSessions();
        allWords = [];
        for (const sess of sessionsAll) {
          const vocab = await dbGetSessionVocab(sess.id);
          const mIds = await dbGetMastered(sess.id);
          const fIds = await dbGetFlagged(sess.id);
          vocab.forEach((item) =>
            allWords.push({
              id:
                sess.id === currentSessionId
                  ? item.id
                  : sess.id + ":" + item.id,
              _realId: item.id,
              _sessId: sess.id,
              _sessName: sess.name,
              _isOtherSess: sess.id !== currentSessionId,
              fullDisplayGerman: item.originalGerman,
              mainGerman: item.mainGerman || item.originalGerman,
              meaning: item.meaning,
              wordType: item.wordType || "",
              example: item.example || "",
              isAnsweredCorrectly: false,
              isMastered: mIds.has(item.id),
              isFlagged: fIds.has(item.id),
            }),
          );
        }
      } else {
        allWords = await buildFullListAll();
      }

      let filtered = allWords;
      if (isSearching) {
        const q = normSearch(sidebarFilter);
        filtered = filtered.filter(
          (v) =>
            normSearch(v.fullDisplayGerman).includes(q) ||
            normSearch(v.meaning).includes(q),
        );
      }
      if (sidebarFilterTab === "active")
        filtered = filtered.filter((v) => !v.isMastered);
      else if (sidebarFilterTab === "mastered")
        filtered = filtered.filter((v) => v.isMastered);
      else if (sidebarFilterTab === "flagged")
        filtered = filtered.filter((v) => v.isFlagged);
      if (sidebarTypeFilter === "n")
        filtered = filtered.filter((v) => {
          const wt = (v.wordType || "").toLowerCase();
          return wt === "n" || wt.startsWith("n ");
        });
      else if (sidebarTypeFilter === "v")
        filtered = filtered.filter((v) => {
          const wt = (v.wordType || "").toLowerCase();
          return wt === "v" || wt.startsWith("v ");
        });
      else if (sidebarTypeFilter === "other")
        filtered = filtered.filter((v) => {
          const wt = (v.wordType || "").toLowerCase();
          return (
            wt !== "n" &&
            !wt.startsWith("n ") &&
            wt !== "v" &&
            !wt.startsWith("v ")
          );
        });

      // Map sessId → tên phiên rút gọn để hiển thị prefix
      let sessOrderMap = {};
      if (isSearchingAllSessions && sessionsAll) {
        sessionsAll.forEach((s) => {
          // Rút gọn tên phiên: bỏ emoji đầu, lấy phần số/chữ ngắn gọn
          let short = s.name.replace(/^[\p{Emoji}\s]+/u, "").trim();
          // Nếu tên có dạng "Bài 13" hoặc "bài 13" thì lấy số
          const numMatch = short.match(/\d+/);
          if (numMatch) short = numMatch[0];
          else if (short.length > 6) short = short.slice(0, 6);
          sessOrderMap[s.id] = short || s.name.slice(0, 6);
        });
      }

      const totalPages = Math.max(
        1,
        Math.ceil(filtered.length / SIDEBAR_PER_PAGE),
      );
      if (sidebarPage > totalPages) sidebarPage = totalPages;
      const start = (sidebarPage - 1) * SIDEBAR_PER_PAGE,
        page = filtered.slice(start, start + SIDEBAR_PER_PAGE);
      const wl = document.getElementById("wordList");
      if (!wl) return;
      const answeredIds = new Set(
        currentQuestionsList
          .filter((q) => q.isAnsweredCorrectly)
          .map((q) => q.id),
      );
      const currentId = currentQuestionsList[currentQIndex]?.id;
      const mobile = isMobileView(),
        bulkMode = window._bulkMode?.(),
        bulkSelected = window._bulkSelected;

      // Helper: tạo display name (thêm prefix số bài nếu từ phiên khác)
      const displayName = (v) => {
        if (v._isOtherSess && sessOrderMap[v._sessId]) {
          return `(${sessOrderMap[v._sessId]}) ${v.fullDisplayGerman}`;
        }
        return v.fullDisplayGerman;
      };

      if (mobile) {
        wl.innerHTML = page
          .map((v) => {
            const isActive = v.id === currentId,
              isDone = answeredIds.has(v.id),
              isMastered = v.isMastered,
              isFlagged = v.isFlagged;
            const cc = isMastered
              ? "mastered-check"
              : isFlagged
                ? "flagged-check"
                : isDone
                  ? "done"
                  : "";
            const ct = isMastered
              ? '<i class="fa-solid fa-star"></i>'
              : isFlagged
                ? '<i class="fa-solid fa-star"></i>'
                : isDone
                  ? '<i class="fa-solid fa-check"></i>'
                  : "";
            const cbChecked = bulkSelected?.has(v.id) ? " checked" : "";
            const masterBtn = isMastered
              ? `<button class="wab unmaster" data-action="unmaster" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}"><i class="fa-solid fa-rotate-left"></i> Học lại</button>`
              : `<button class="wab master" data-action="master" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}"><i class="fa-solid fa-circle-check"></i> Thuộc</button>`;
            const flagBtn = isFlagged
              ? `<button class="wab flag-action" data-action="unflag" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}"><i class="fa-regular fa-star"></i> Bỏ chú ý</button>`
              : `<button class="wab flag-action" data-action="flag" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}"><i class="fa-solid fa-star"></i> Chú ý</button>`;
            const sessionBtns =
              currentSource === "session"
                ? `<button class="wab del" data-action="delete" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}"><i class="fa-solid fa-trash"></i> Xóa</button>`
                : "";
            const nameHtml = v._isOtherSess
              ? `<span style="color:var(--tx3);font-size:0.72rem;font-weight:600;margin-right:3px;">(${sessOrderMap[v._sessId] || "?"})</span>${escapeHtml(v.fullDisplayGerman)}`
              : escapeHtml(v.fullDisplayGerman);
            return `<div class="word-item${isActive ? " active" : ""}${isDone && !isMastered ? " correct" : ""}${isMastered ? " mastered" : ""}${isFlagged ? " flagged" : ""}${v._isOtherSess ? " other-sess" : ""}" data-id="${v.id}" data-realid="${v._realId || v.id}" data-mastered="${isMastered}" data-flagged="${isFlagged}"><div class="word-item-main" data-id="${v.id}"><input type="checkbox" class="word-item-cb" data-id="${v.id}"${cbChecked}><div class="word-item-circle${bulkSelected.has(v.id) ? ' checked' : ''}" data-id="${v.id}"></div><div class="word-check ${cc}">${ct}</div><div class="word-text flex-1 min-w-0"><div class="word-de font-bold text-[0.9rem] overflow-hidden text-ellipsis whitespace-nowrap">${nameHtml}${v.wordType ? `<span class="word-type-badge">${escapeHtml(v.wordType)}</span>` : ""}</div><div class="word-vi text-[0.76rem] text-[#6e7681] overflow-hidden text-ellipsis whitespace-nowrap font-mono mt-px">${escapeHtml(v.meaning)}</div></div><div class="word-status-chips"><span class="word-mastered-dot"><i class="fa-solid fa-star"></i> thuộc</span><span class="word-flagged-dot"><i class="fa-solid fa-star"></i> chú ý</span></div><button class="word-expand-btn" data-expand="${v.id}">•••</button></div><div class="word-item-actions-row"><button class="wab edit" data-action="edit" data-id="${v.id}" data-realid="${v._realId || v.id}" data-src="${currentSource}" data-sessid="${v._sessId || ""}"><i class="fa-solid fa-pen-to-square"></i> Sửa</button>${masterBtn}${flagBtn}${sessionBtns}</div></div>`;
          })
          .join("");

        wl.querySelectorAll(".word-item-cb").forEach((cb) =>
          cb.addEventListener("change", (e) => {
            e.stopPropagation();
            const id = e.target.dataset.id;
            if (e.target.checked) bulkSelected.add(id);
            else bulkSelected.delete(id);
            // Sync circle visual
            const circle = e.target.parentElement?.querySelector('.word-item-circle[data-id="' + id + '"]');
            if (circle) circle.classList.toggle('checked', e.target.checked);
            window._updateBulkCount?.();
          }),
        );

        wl.querySelectorAll(".word-item").forEach((el) => {
          el.querySelector(".word-item-main")?.addEventListener(
            "click",
            async (e) => {
              const expandBtn = e.target.closest(".word-expand-btn");
              if (expandBtn) {
                e.stopPropagation();
                const isOpen = el.classList.contains("open-actions");
                wl.querySelectorAll(".word-item.open-actions").forEach(
                  (o) => {
                    if (o !== el) o.classList.remove("open-actions");
                  },
                );
                el.classList.toggle("open-actions", !isOpen);
                return;
              }
              if (bulkMode) {
                const cb = el.querySelector(".word-item-cb");
                if (cb) {
                  cb.checked = !cb.checked;
                  const id = cb.dataset.id;
                  if (cb.checked) bulkSelected.add(id); else bulkSelected.delete(id);
                  const circle = el.querySelector('.word-item-circle[data-id="' + id + '"]');
                  if (circle) circle.classList.toggle('checked', cb.checked);
                  window._updateBulkCount?.();
                  return;
                }
              }
              const id = el.dataset.id;
              const isMasteredEl = el.dataset.mastered === "true";
              // Phát âm ngay lập tức — lấy từ currentQuestionsList trước (nhanh hơn, giữ user gesture)
              const quickWord =
                currentQuestionsList.find(
                  (q) => (q._realId || q.id) === id,
                ) || currentQuestionsList.find((q) => q.id === id);
              if (quickWord) {
                unlockTTS();
                speakText(quickWord.fullDisplayGerman);
              } else {
                // fallback: tìm trong toàn bộ danh sách
                unlockTTS();
                buildFullListAll().then((allW) => {
                  const word = allW.find((v) => v.id === id);
                  if (word) speakText(word.fullDisplayGerman);
                });
              }
              if (isMasteredEl) return;
              const allW = await buildFullListAll();
              const idx = currentQuestionsList.findIndex((q) => q.id === id);
              if (idx !== -1) {
                currentQIndex = idx;
                currentQuestionsList[idx].isAnsweredCorrectly = false;
                window._noFocusNext = true;
                renderExercise();
                await renderSidebar();
              }
            },
          );
        });
        // Delegated listener for .wab action buttons (mobile) — remove previous before re-attaching
        if (wl._mobileWabHandler) {
          wl.removeEventListener("click", wl._mobileWabHandler);
        }
        wl._mobileWabHandler = function (e) {
          const btn = e.target.closest(".wab[data-action]");
          if (!btn) return;
          e.stopPropagation();
          const item = btn.closest(".word-item");
          if (item) item.classList.remove("open-actions");
          handleSidebarAction(btn);
        };
        wl.addEventListener("click", wl._mobileWabHandler);
      } else {
        wl.innerHTML = page
          .map((v) => {
            const isActive = v.id === currentId,
              isDone = answeredIds.has(v.id),
              isMastered = v.isMastered,
              isFlagged = v.isFlagged;
            const cc = isMastered
              ? "mastered-check"
              : isFlagged
                ? "flagged-check"
                : isDone
                  ? "done"
                  : "";
            const ct = isMastered
              ? '<i class="fa-solid fa-star"></i>'
              : isFlagged
                ? '<i class="fa-solid fa-star"></i>'
                : isDone
                  ? '<i class="fa-solid fa-check"></i>'
                  : "";
            const cbChecked = bulkSelected?.has(v.id) ? " checked" : "";
            const nameHtml = v._isOtherSess
              ? `<span style="color:var(--accent-blue,#58a6ff);font-size:0.7rem;font-weight:700;margin-right:3px;opacity:.85;">(${sessOrderMap[v._sessId] || "?"})</span>${escapeHtml(v.fullDisplayGerman)}`
              : escapeHtml(v.fullDisplayGerman);
            return `<div class="word-item${isActive ? " active" : ""}${isDone && !isMastered ? " correct" : ""}${isMastered ? " mastered" : ""}${isFlagged ? " flagged" : ""}${v._isOtherSess ? " other-sess" : ""}" data-id="${v.id}" data-realid="${v._realId || v.id}" data-mastered="${isMastered}" data-flagged="${isFlagged}"><input type="checkbox" class="word-item-cb" data-id="${v.id}"${cbChecked}><div class="word-item-circle${bulkSelected.has(v.id) ? ' checked' : ''}" data-id="${v.id}"></div><div class="word-check ${cc}">${ct}</div><div class="word-text flex-1 min-w-0"><div class="word-de text-[0.84rem] font-semibold overflow-hidden text-ellipsis whitespace-nowrap">${nameHtml}${v.wordType ? `<span class="word-type-badge">${escapeHtml(v.wordType)}</span>` : ""}</div><div class="word-vi text-[0.75rem] text-[#6e7681] overflow-hidden text-ellipsis whitespace-nowrap font-mono">${escapeHtml(v.meaning)}</div></div>${isFlagged ? `<span class="flagged-badge"><i class="fa-solid fa-star"></i> chú ý</span>` : ""}${isMastered ? `<span class="mastered-badge"><i class="fa-solid fa-star"></i> thuộc</span>` : ""}<div class="word-actions"><button class="wa-btn" data-action="edit" data-id="${v.id}" data-realid="${v._realId || v.id}" data-src="${currentSource}" data-sessid="${v._sessId || ""}" title="Sửa"><i class="fa-solid fa-pen"></i></button>${isMastered ? `<button class="wa-btn" data-action="unmaster" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}" title="Học lại"><i class="fa-solid fa-rotate-left"></i></button>` : `<button class="wa-btn" data-action="master" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}" title="Thuộc rồi"><i class="fa-solid fa-circle-check"></i></button>`}${isFlagged ? `<button class="wa-btn flag" data-action="unflag" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}" title="Bỏ chú ý"><i class="fa-regular fa-star"></i></button>` : `<button class="wa-btn flag" data-action="flag" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}" title="Đánh dấu chú ý"><i class="fa-solid fa-star"></i></button>`}${currentSource === "session" ? `<button class="wa-btn del" data-action="delete" data-id="${v.id}" data-realid="${v._realId || v.id}" data-sessid="${v._sessId || ""}" title="Xóa"><i class="fa-solid fa-trash"></i></button>` : ""}</div></div>`;
          })
          .join("");

        wl.querySelectorAll(".word-item-cb").forEach((cb) =>
          cb.addEventListener("change", (e) => {
            e.stopPropagation();
            const id = e.target.dataset.id;
            if (e.target.checked) bulkSelected.add(id);
            else bulkSelected.delete(id);
            const circle = e.target.parentElement?.querySelector('.word-item-circle[data-id="' + id + '"]');
            if (circle) circle.classList.toggle('checked', e.target.checked);
            window._updateBulkCount?.();
          }),
        );

        // Single delegated listener on wl — avoids stale per-element handlers.
        // Remove previous listener before re-attaching on every render.
        if (wl._desktopClickHandler) {
          wl.removeEventListener("click", wl._desktopClickHandler);
        }
        wl._desktopClickHandler = async function (e) {
          // Action buttons (.wa-btn): edit, master/unmaster, flag/unflag, delete
          const btn = e.target.closest(".wa-btn");
          if (btn) {
            e.stopPropagation();
            handleSidebarAction(btn);
            return;
          }
          // Clicks on word item body (not on action buttons)
          const el = e.target.closest(".word-item");
          if (!el) return;
          if (bulkMode) {
            const cb = el.querySelector(".word-item-cb");
            if (cb) {
              cb.checked = !cb.checked;
              const id = cb.dataset.id;
              if (cb.checked) bulkSelected.add(id); else bulkSelected.delete(id);
              const circle = el.querySelector('.word-item-circle[data-id="' + id + '"]');
              if (circle) circle.classList.toggle('checked', cb.checked);
              window._updateBulkCount?.();
              return;
            }
          }
          const id = el.dataset.id,
            isMastered = el.dataset.mastered === "true";
          // Phát âm ngay — lấy từ currentQuestionsList trước để giữ user gesture
          const quickWord =
            currentQuestionsList.find((q) => (q._realId || q.id) === id) ||
            currentQuestionsList.find((q) => q.id === id);
          if (quickWord) {
            unlockTTS();
            speakText(quickWord.fullDisplayGerman);
          } else {
            unlockTTS();
            buildFullListAll().then((allW) => {
              const word = allW.find((v) => v.id === id);
              if (word) speakText(word.fullDisplayGerman);
            });
          }
          if (isMastered) return;
          const idx = currentQuestionsList.findIndex((q) => q.id === id);
          if (idx !== -1) {
            currentQIndex = idx;
            currentQuestionsList[idx].isAnsweredCorrectly = false;
            window._noFocusNext = true;
            renderExercise();
            await renderSidebar();
          }
        };
        wl.addEventListener("click", wl._desktopClickHandler);
      }

      const totalAll = allWords.length,
        totalMastered = allWords.filter((v) => v.isMastered).length,
        totalFlagged = allWords.filter((v) => v.isFlagged).length;
      document.getElementById("sidebarCount").innerHTML =
        `${totalAll} từ (${totalMastered} thuộc · ${totalFlagged} <i class="fa-solid fa-star"></i>)`;
      document.getElementById("practiceCountNum").textContent =
        currentQuestionsList.length;

      const pager = document.getElementById("sidebarPager");
      if (!pager) return;
      if (totalPages <= 1) {
        pager.innerHTML = "";
        return;
      }
      let ph = "";
      if (sidebarPage > 1)
        ph += `<button class="pg-btn" data-p="${sidebarPage - 1}">‹</button>`;
      const s2 = Math.max(1, sidebarPage - 2),
        e2 = Math.min(totalPages, sidebarPage + 2);
      for (let i = s2; i <= e2; i++)
        ph += `<button class="pg-btn${i === sidebarPage ? " active" : ""}" data-p="${i}">${i}</button>`;
      if (sidebarPage < totalPages)
        ph += `<button class="pg-btn" data-p="${sidebarPage + 1}">›</button>`;
      pager.innerHTML = ph;
      pager.querySelectorAll(".pg-btn").forEach((b) =>
        b.addEventListener("click", () => {
          sidebarPage = parseInt(b.dataset.p);
          renderSidebar();
        }),
      );
    }

    function handleSidebarAction(btn) {
      const action = btn.dataset.action,
        // data-realid holds the real word id (without sessId prefix) for merged sessions
        id = btn.dataset.realid || btn.dataset.id,
        sessId = btn.dataset.sessid || currentSessionId;
      if (action === "edit")
        openEditWord(id, btn.dataset.src || currentSource, sessId);
      else if (action === "master") markWordMasteredInSess(id, sessId);
      else if (action === "unmaster") unmarkWordMasteredInSess(id, sessId);
      else if (action === "flag") markWordFlaggedInSess(id, sessId);
      else if (action === "unflag") unmarkWordFlaggedInSess(id, sessId);
      else if (action === "delete") deleteWord(id, sessId);
    }
    // Expose sidebar actions to window for inline onclick handlers
    window._sidebarEdit = (id, src, sessId) => openEditWord(id, src || currentSource, sessId || currentSessionId);
    window._sidebarMaster = (id, sessId) => markWordMasteredInSess(id, sessId || currentSessionId);
    window._sidebarUnmaster = (id, sessId) => unmarkWordMasteredInSess(id, sessId || currentSessionId);
    window._sidebarFlag = (id, sessId) => markWordFlaggedInSess(id, sessId || currentSessionId);
    window._sidebarUnflag = (id, sessId) => unmarkWordFlaggedInSess(id, sessId || currentSessionId);
    window._sidebarDelete = (id, sessId) => deleteWord(id, sessId || currentSessionId);

    // Khi đang học theo "Chọn từ luyện tập" với tiêu chí lọc đã lưu (window.customFilterCriteria),
    // hàm này chạy lại tiêu chí đó trên dữ liệu mới nhất để tự động thêm/bớt từ khỏi danh sách
    // đang học — ví dụ: bỏ chú ý một từ ở ngoài thì từ đó cũng tự động bị loại khỏi batch đang học.
    async function syncCustomSelectionWithFilter() {
      if (!isCustomMode || !window.customFilterCriteria) return;
      try {
        const _inputSnap = _snapshotAnswerInput();
        const { mv, tv, sv } = window.customFilterCriteria;
        const allWords = await buildFullListAll();
        let list = allWords;
        if (mv === "active") list = list.filter((v) => !v.isMastered);
        else if (mv === "mastered") list = list.filter((v) => v.isMastered);
        else if (mv === "flagged") list = list.filter((v) => v.isFlagged);
        if (tv === "n")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return wt === "n" || wt.startsWith("n ");
          });
        else if (tv === "v")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return wt === "v" || wt.startsWith("v ");
          });
        else if (tv === "other")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return (
              wt !== "n" &&
              !wt.startsWith("n ") &&
              wt !== "v" &&
              !wt.startsWith("v ")
            );
          });
        if (sv)
          list = list.filter(
            (v) =>
              v.fullDisplayGerman.toLowerCase().includes(sv) ||
              v.meaning.toLowerCase().includes(sv),
          );
        const newIds = new Set(list.map((i) => i.id));
        window.selectedWordIds = [...newIds];
        window.customMaster = list;

        // Cập nhật currentQuestionsList tại chỗ — giữ nguyên thứ tự & vị trí câu hỏi hiện tại
        let removedBeforeCurrent = 0;
        const newQList = [];
        currentQuestionsList.forEach((item, idx) => {
          if (!newIds.has(item.id)) {
            if (idx < currentQIndex) removedBeforeCurrent++;
            return;
          }
          newQList.push(item);
        });
        const existingIds = new Set(newQList.map((i) => i.id));
        // Chỉ nối thêm từ mới vào batch nếu còn "chỗ trống" trong giới hạn wordLimit hiện tại —
        // tránh làm patch/batch phình to vượt quá giới hạn đã đặt.
        if (wordLimit > 0) {
          let room = Math.max(0, wordLimit - newQList.length);
          for (const item of list) {
            if (room <= 0) break;
            if (!existingIds.has(item.id)) {
              newQList.push({ ...item, isAnsweredCorrectly: false });
              existingIds.add(item.id);
              room--;
            }
          }
        } else {
          list.forEach((item) => {
            if (!existingIds.has(item.id))
              newQList.push({ ...item, isAnsweredCorrectly: false });
          });
        }
        currentQuestionsList = newQList;
        if (currentQuestionsList.length === 0) currentQIndex = 0;
        else {
          currentQIndex = Math.max(0, currentQIndex - removedBeforeCurrent);
          if (currentQIndex >= currentQuestionsList.length)
            currentQIndex = currentQuestionsList.length - 1;
        }
        renderExercise();
        _restoreAnswerInput(_inputSnap);
        updateBatchBar(list);
        saveAppState();
      } catch (err) {
        console.error("[syncCustomSelectionWithFilter]", err);
      }
    }

    async function markWordMasteredInSess(wordId, sessId) {
      try {
        await dbMarkMastered(sessId, wordId);
        // Đã thuộc thì không thể còn ở trạng thái "cần chú ý" nữa
        await dbUnmarkFlagged(sessId, wordId);
      } catch (err) {
        console.error("markWordMastered error:", err);
        showToast("❌ Lỗi đánh dấu thuộc: " + (err?.message || err));
        return;
      }
      if (sessId === currentSessionId) {
        masteredIds.add(wordId);
        flaggedIds.delete(wordId);
      }
      const flagQ = currentQuestionsList.find(
        (q) => q.id === wordId || q._realId === wordId,
      );
      if (flagQ) flagQ.isFlagged = false;
      _updateFlagUIInPlace(wordId, false);
      _updateSidebarItemFlag(wordId, false);
      // Chỉ xóa khỏi patch đang học — KHÔNG xóa khỏi customMaster/selectedWordIds
      currentQuestionsList = currentQuestionsList.filter(
        (q) => q.id !== wordId && q._realId !== wordId,
      );
      if (currentQIndex >= currentQuestionsList.length && currentQIndex > 0)
        currentQIndex--;
      showToast("✅ Đã đánh dấu thuộc lòng!");
      renderExercise();
      await renderSidebar();
      await syncCustomSelectionWithFilter();
    }
    async function unmarkWordMasteredInSess(wordId, sessId) {
      try {
        await dbUnmarkMastered(sessId, wordId);
      } catch (err) {
        console.error("unmarkWordMastered error:", err);
        showToast("❌ Lỗi bỏ đánh dấu: " + (err?.message || err));
        return;
      }
      if (sessId === currentSessionId) masteredIds.delete(wordId);
      const item = currentQuestionsList.find(
        (q) => (q._realId || q.id) === wordId,
      );
      if (item) item.isMastered = false;
      showToast("↩️ Đã đưa lại danh sách học");
      if (onlyUnmastered) {
        // Từ vừa bỏ thuộc cần được đưa lại vào danh sách đang học (nếu đang lọc "chỉ từ chưa thuộc"),
        // nhưng giữ nguyên vị trí câu hỏi hiện tại thay vì reload về đầu batch.
        await mergeVocabChangesInPlace();
      } else {
        renderExercise();
        await renderSidebar();
      }
      await syncCustomSelectionWithFilter();
    }
    async function markWordFlaggedInSess(wordId, sessId) {
      try {
        await dbMarkFlagged(sessId, wordId);
        // Cần chú ý thì không thể còn ở trạng thái "đã thuộc" nữa
        await dbUnmarkMastered(sessId, wordId);
      } catch (err) {
        console.error("markWordFlagged error:", err);
        showToast("❌ Lỗi đánh dấu chú ý: " + (err?.message || err));
        return;
      }
      if (sessId === currentSessionId) {
        flaggedIds.add(wordId);
        masteredIds.delete(wordId);
      }
      const q = currentQuestionsList.find(
        (q) => q.id === wordId || q._realId === wordId,
      );
      if (q) {
        q.isFlagged = true;
        q.isMastered = false;
      }
      showToast("⭐ Đã đánh dấu cần chú ý!");
      _updateFlagUIInPlace(wordId, true);
      _updateSidebarItemFlag(wordId, true);
      await syncCustomSelectionWithFilter();
      if (onlyUnmastered) {
        // Từ vừa đánh dấu chú ý bị bỏ thuộc → nếu trước đó ẩn khỏi danh sách (đã thuộc),
        // giờ cần đưa lại vào danh sách đang học, nhưng giữ nguyên vị trí câu hỏi hiện tại
        // thay vì reload về đầu batch.
        await mergeVocabChangesInPlace();
      } else {
        await renderSidebar();
      }
    }
    async function unmarkWordFlaggedInSess(wordId, sessId) {
      try {
        await dbUnmarkFlagged(sessId, wordId);
      } catch (err) {
        console.error("unmarkWordFlagged error:", err);
        showToast("❌ Lỗi bỏ chú ý: " + (err?.message || err));
        return;
      }
      if (sessId === currentSessionId) flaggedIds.delete(wordId);
      const q = currentQuestionsList.find(
        (q) => q.id === wordId || q._realId === wordId,
      );
      if (q) q.isFlagged = false;
      showToast("☆ Đã bỏ đánh dấu chú ý");
      _updateFlagUIInPlace(wordId, false);
      _updateSidebarItemFlag(wordId, false);
      await syncCustomSelectionWithFilter();
    }
    function _updateFlagUIInPlace(wordId, isFlagged) {
      const q =
        currentQuestionsList[
        currentQIndex % Math.max(currentQuestionsList.length, 1)
        ];
      if (!q || (q.id !== wordId && q._realId !== wordId)) return;
      const prompt = document.querySelector(".ex-prompt");
      if (prompt) {
        prompt.classList.toggle("state-flagged", isFlagged);
        if (!isFlagged && !q.isAnsweredCorrectly)
          prompt.classList.add("state-normal");
        if (!isFlagged && q.isAnsweredCorrectly)
          prompt.classList.add("state-correct");
      }
      const typeTagEl = document.querySelector(".ex-prompt .ex-type-row");
      if (typeTagEl) {
        let indicator = typeTagEl.querySelector(".flag-indicator");
        if (isFlagged && !indicator) {
          indicator = document.createElement("span");
          indicator.className = "flag-indicator";
          indicator.title = "Cần chú ý";
          indicator.innerHTML = '<i class="fa-solid fa-star"></i>';
          typeTagEl.appendChild(indicator);
        } else if (!isFlagged && indicator) indicator.remove();
      }
      const flagBtn = document.getElementById("flagWordBtn");
      if (flagBtn) {
        if (isFlagged) {
          flagBtn.className = "exbtn unflag-btn";
          flagBtn.innerHTML = '<i class="fa-regular fa-star"></i> Bỏ chú ý';
        } else {
          flagBtn.className = "exbtn flag-btn";
          flagBtn.innerHTML = '<i class="fa-solid fa-star"></i> Chú ý';
        }
      }
    }
    function _updateSidebarItemFlag(wordId, isFlagged) {
      // Khi gộp phiên, data-id là "sessId:wordId" — tìm bằng data-realid trước, fallback data-id
      const el =
        document.querySelector(`.word-item[data-realid="${CSS.escape(wordId)}"]`) ||
        document.querySelector(`.word-item[data-id="${CSS.escape(wordId)}"]`);
      if (!el) return;
      el.dataset.flagged = String(isFlagged);
      el.classList.toggle("flagged", isFlagged);
      if (!isMobileView()) {
        const badge = el.querySelector(".flagged-badge");
        if (isFlagged && !badge) {
          const b = document.createElement("span");
          b.className = "flagged-badge";
          b.innerHTML = '<i class="fa-solid fa-star"></i> chú ý';
          const mb = el.querySelector(".mastered-badge");
          if (mb) el.insertBefore(b, mb);
          else {
            const ac = el.querySelector(".word-actions");
            if (ac) el.insertBefore(b, ac);
          }
        } else if (!isFlagged && badge) badge.remove();
        const fw = el.querySelector(".wa-btn.flag");
        if (fw) {
          fw.dataset.action = isFlagged ? "unflag" : "flag";
          fw.title = isFlagged ? "Bỏ chú ý" : "Đánh dấu chú ý";
          fw.innerHTML = isFlagged
            ? '<i class="fa-regular fa-star"></i>'
            : '<i class="fa-solid fa-star"></i>';
        }
      } else {
        const dot = el.querySelector(".word-flagged-dot");
        if (dot) dot.style.display = isFlagged ? "inline-block" : "none";
        const checkDot = el.querySelector(".word-check"),
          isMastered = el.dataset.mastered === "true";
        if (checkDot && !isMastered) {
          checkDot.className = isFlagged
            ? "word-check flagged-check"
            : "word-check";
          checkDot.innerHTML = isFlagged
            ? '<i class="fa-solid fa-star"></i>'
            : "";
        }
        const fw = el.querySelector(".wab.flag-action");
        if (fw) {
          fw.dataset.action = isFlagged ? "unflag" : "flag";
          fw.innerHTML = isFlagged
            ? '<i class="fa-regular fa-star"></i> Bỏ chú ý'
            : '<i class="fa-solid fa-star"></i> Chú ý';
        }
      }
    }
    async function toggleFlagCurrentWord() {
      if (!currentQuestionsList.length) return;
      const q =
        currentQuestionsList[currentQIndex % currentQuestionsList.length];
      if (!q) return;
      const sessId = q._sessId || currentSessionId,
        realId = q._realId || q.id;
      if (q.isFlagged) await unmarkWordFlaggedInSess(realId, sessId);
      else await markWordFlaggedInSess(realId, sessId);
    }
    function updateStatsBar() {
      const pct =
        stats.totalAttempts === 0
          ? 0
          : Math.round((stats.correctCount / stats.totalAttempts) * 100);
      ["statCorrect", "mStatCorrect"].forEach((id) => {
        const e = document.getElementById(id);
        if (e) e.textContent = stats.correctCount;
      });
      ["statTotal", "mStatTotal"].forEach((id) => {
        const e = document.getElementById(id);
        if (e) e.textContent = stats.totalAttempts;
      });
      ["statPct", "mStatPct"].forEach((id) => {
        const e = document.getElementById(id);
        if (e) e.textContent = pct + "%";
      });
    }
    function _getPromptStateClass(q) {
      if (!q) return "state-normal";
      if (q.isFlagged) return "state-flagged";
      if (q.isMastered) return "state-mastered";
      if (q.isAnsweredCorrectly) return "state-correct";
      return "state-normal";
    }

    function getReducedTarget(q) {
      // Khi tắt strict: mọi từ chỉ cần nhập phần trước dấu (
      if (!strictVocabCheck) {
        const parenIdx = q.fullDisplayGerman.indexOf("(");
        if (parenIdx >= 0) {
          return q.fullDisplayGerman.slice(0, parenIdx).trim() || null;
        }
      }
      return null;
    }
    function checkWriteAnswer(val, q) {
      const reduced = getReducedTarget(q);
      if (reduced !== null) return isSmartMatch(val, reduced, false);
      return isSmartMatch(val, q.fullDisplayGerman);
    }

    // _mixRound: 0 = fullWord, 1 = fullMeaning, 2 = fullSentence — đổi sau mỗi lượt hết batch
    let _mixRound = 0;
    const MIX_TYPES = ["fullWord", "fullMeaning"];

    // Trả về type thực tế của câu hỏi hiện tại
    function getEffectiveType(q) {
      if (currentExerciseType === "mixedRandom") {
        return MIX_TYPES[_mixRound] || "fullWord";
      }
      return currentExerciseType;
    }

    // Ẩn/hiện options loại bài tập tùy mode nghe
    function _syncTypeSelectToMode(mode) {
      ["exerciseTypeSelect", "mobExerciseTypeSelect", "exerciseTypeSelectModal"].forEach((id) => {
        const sel = document.getElementById(id);
        if (!sel) return;
        const optWord = sel.querySelector('option[value="fullWord"]');
        const optMeaning = sel.querySelector('option[value="fullMeaning"]');
        if (mode === "listen") {
          if (optWord) optWord.style.display = "none";
          if (optMeaning) optMeaning.style.display = "none";
          // Force về mixedRandom
          sel.value = "mixedRandom";
        } else {
          if (optWord) optWord.style.display = "";
          if (optMeaning) optMeaning.style.display = "";
        }
      });
      if (mode === "listen" && currentExerciseType !== "mixedRandom") {
        changeExType("mixedRandom");
      }
    }

    function onInputChange(val, input) {
      if (isWaitingForAutoNext || !currentQuestionsList.length) return;
      const q =
        currentQuestionsList[currentQIndex % currentQuestionsList.length];
      if (!q) return;

      // Kiểm tra đáp án tuỳ mode
      let ok = false;
      const effectiveType = getEffectiveType(q);
      if (exerciseMode === "listen") {
        // Listen mode: chấp nhận nghĩa tiếng Việt, hoặc từ tiếng Đức.
        // Danh từ có số nhiều: phần tiếng Đức phải nhập đủ cả số ít lẫn số nhiều.
        // Nghĩa tiếng Việt vẫn chấp nhận bình thường.
        // Khi tắt strictVocabCheck: động từ chỉ cần nhập phần trước dấu (
        const reduced = getReducedTarget(q);
        if (reduced !== null) {
          ok = isMeaningMatch(val, q.meaning) || isSmartMatch(val, reduced, false);
        } else {
          ok = isMeaningMatch(val, q.meaning) || isOriginalWordMatch(val, q.fullDisplayGerman);
        }
      } else if (effectiveType === "fullWord") {
        ok = checkWriteAnswer(val, q);
      } else if (effectiveType === "fullMeaning") {
        ok = isMeaningMatch(val, q.meaning);
      } else if (effectiveType === "fullSentence") {
        ok = isSentenceMatch(val, q.example);
      } else {
        ok = isSmartMatch(val, q.fullDisplayGerman);
      }

      // Hint target tuỳ mode
      const hintBox = document.getElementById("charHintBox");
      if (hintBox && hintBox.classList.contains("visible")) {
        if (effectiveType === "fullSentence") {
          hintBox.innerHTML = q.example
            ? `<span style="color:var(--tx3);font-size:0.8rem;">Câu ví dụ: </span><span style="font-family:'DM Mono',monospace;color:var(--tx);">${escapeHtml(q.example)}</span>`
            : `<span style="color:var(--tx3);font-size:0.8rem;">Chưa có câu mẫu — câu phải chứa từ: </span><span style="font-family:'DM Mono',monospace;color:#58a6ff;font-weight:700;">${escapeHtml(q.mainGerman || q.fullDisplayGerman)}</span>`;
        } else if (exerciseMode === "listen") {
          // Listen mode: hiện "từ tiếng Đức — nghĩa", kèm câu ví dụ (nếu có) để hỗ trợ nghe/viết lại
          const _exampleLine = q.example
            ? `<div style="margin-top:6px;"><span style="color:var(--tx3);font-size:0.8rem;">Câu ví dụ: </span><span style="font-family:'DM Mono',monospace;color:var(--tx);">${escapeHtml(q.example)}</span></div>`
            : "";
          hintBox.innerHTML = `<div><span style="font-family:'DM Mono',monospace;color:var(--tx);font-weight:600">${escapeHtml(q.fullDisplayGerman)}</span><span style="color:var(--tx3);margin:0 6px">—</span><span style="color:var(--tx2)">${escapeHtml(q.meaning)}</span></div>${_exampleLine}`;
        } else {
          const target =
            effectiveType === "fullMeaning"
              ? q.meaning
              : q.fullDisplayGerman;
          hintBox.innerHTML = buildCharHint(val, target);
        }
      }

      if (ok) {
        input.classList.add("correct-border");
        input.classList.remove("wrong-border");
        if (!q.isAnsweredCorrectly) {
          q.isAnsweredCorrectly = true;
          stats.totalAttempts++;
          stats.correctCount++;
          updateStatsBar();
          const si = document.querySelector(
            `.word-item[data-id="${q.id}"] .word-check`,
          );
          if (si) {
            si.classList.add("done");
            si.innerHTML = '<i class="fa-solid fa-check"></i>';
          }

          // Chỉ phát âm khi KHÔNG phải listen mode
          if (soundEnabled && exerciseMode !== "listen") {
            if (effectiveType === "fullSentence" && q.example) {
              speakText(getGermanExample(q.example));
            } else {
              const reduced = getReducedTarget(q);
              speakText(reduced !== null ? reduced : q.fullDisplayGerman);
            }
          }

          const prompt = document.querySelector(".ex-prompt");
          if (prompt && !q.isFlagged) {
            prompt.classList.remove("state-normal", "state-flagged");
            prompt.classList.add("state-correct");
          }
          const exBox = document.getElementById("exampleRevealBox");
          if (exBox && effectiveType !== "fullSentence" && q.example) {
            exBox.innerHTML = `<div class="text-[0.65rem] font-bold uppercase tracking-widest text-[#6e7681] mb-1"><i class="fa-solid fa-pen-to-square"></i> Ví dụ</div>${escapeHtml(q.example)}`;
            exBox.classList.add("visible");
          }
        }
        if (!studyMode && autoAdvanceOnCorrect) {
          isWaitingForAutoNext = true;
          setTimeout(() => {
            moveNext(input, false);
            isWaitingForAutoNext = false;
            setTimeout(() => {
              const ni = document.getElementById("dynamicAnswerInput");
              if (ni) ni.focus();
            }, 150);
          }, effectiveType === "fullSentence" ? 1000 : 120);
        }
      } else {
        // Nếu đã từng đúng nhưng giờ sai (user gõ thêm) → reset
        if (q.isAnsweredCorrectly) {
          q.isAnsweredCorrectly = false;
          const prompt = document.querySelector(".ex-prompt");
          if (prompt) {
            prompt.classList.remove("state-correct");
            prompt.classList.add(
              q.isFlagged ? "state-flagged" : "state-normal",
            );
          }
          // Ẩn lại ví dụ đã hiện trước đó — câu trả lời không còn đúng nữa
          const exBox = document.getElementById("exampleRevealBox");
          if (exBox) {
            exBox.classList.remove("visible");
            exBox.innerHTML = "";
          }
        }
        if (val.length > 0 && effectiveType !== "fullSentence") {
          input.classList.remove("correct-border");
          // Chỉ hiện border đỏ khi user đã gõ đủ số ký tự thực (bỏ space/dấu câu)
          // Tránh flash đỏ khi user đang gõ giữa chừng hoặc gõ dấu cách tạm thời
          const _valFlat = val.replace(/[\s\/\\\(\)\[\]\{\}\.,\-_=+!?;:'"<>·]/g, "");
          const _targetFlat = (() => {
            if (exerciseMode === "listen") {
              // Listen mode chấp nhận cả nghĩa lẫn từ gốc → chỉ báo đỏ khi
              // đã gõ vượt độ dài của đáp án DÀI HƠN trong 2 đáp án hợp lệ
              const mFlat = q.meaning.replace(/[\s\/\\\(\)\[\]\{\}\.,\-_=+!?;:'"<>·]/g, "");
              const gFlat = q.fullDisplayGerman.replace(/[\s\/\\\(\)\[\]\{\}\.,\-_=+!?;:'"<>·]/g, "");
              return mFlat.length >= gFlat.length ? mFlat : gFlat;
            }
            const et = getEffectiveType(q);
            const targetRaw = (() => {
              if (et === "fullMeaning") return q.meaning;
              const reduced = getReducedTarget(q);
              return reduced !== null ? reduced : q.fullDisplayGerman;
            })();
            return targetRaw.replace(/[\s\/\\\(\)\[\]\{\}\.,\-_=+!?;:'"<>·]/g, "");
          })();
          if (_valFlat.length >= _targetFlat.length) {
            input.classList.add("wrong-border");
          } else {
            input.classList.remove("wrong-border");
          }
        } else {
          input.classList.remove("correct-border", "wrong-border");
        }
      }
    }

    async function moveNext(input, countAttempt = true, force = false) {
      if (!currentQuestionsList.length) return false;
      const q =
        currentQuestionsList[currentQIndex % currentQuestionsList.length];
      const isMC =
        exerciseMode === "choose";
      if (!isMC && !allowSkip && !force && !q.isAnsweredCorrectly) {
        if (input) {
          input.classList.add("wrong-border");
          setTimeout(() => input.classList.remove("wrong-border"), 500);
        }
        showToast("❌ Hãy nhập đúng đáp án!");
        return false;
      }
      if (studyMode && !isMC) {
        if (!force) {
          q.isAnsweredCorrectly = false;
          if (input) {
            input.value = "";
            input.classList.remove("correct-border", "wrong-border");
            document
              .getElementById("exampleRevealBox")
              ?.classList.remove("visible");
            document
              .getElementById("charHintBox")
              ?.classList.remove("visible");
          }
          if (exerciseMode === "listen") speakForMode(q);
          return true;
        }
        q.isAnsweredCorrectly = false;
        if (input) {
          input.value = "";
          input.classList.remove("correct-border", "wrong-border");
          document
            .getElementById("exampleRevealBox")
            ?.classList.remove("visible");
          document.getElementById("charHintBox")?.classList.remove("visible");
        }
        currentQIndex =
          currentQIndex + 1 >= currentQuestionsList.length
            ? 0
            : currentQIndex + 1;
        renderExercise();
        await renderSidebar();
        saveAppState();
        if (exerciseMode === "listen" && currentQuestionsList[currentQIndex])
          speakForMode(currentQuestionsList[currentQIndex]);
        return true;
      }
      if (input) {
        input.value = "";
        input.classList.remove("correct-border", "wrong-border");
      }
      if (currentQIndex + 1 >= currentQuestionsList.length) {
        currentQIndex = 0;
        shuffleArray(currentQuestionsList);
        currentQuestionsList.forEach((i) => (i.isAnsweredCorrectly = false));
        if (currentExerciseType === "mixedRandom" && exerciseMode !== "listen") {
          _mixRound = (_mixRound + 1) % MIX_TYPES.length;
          const roundLabels = { fullWord: "Lượt Nguyên từ", fullMeaning: "Lượt Nghĩa", fullSentence: "Lượt Nhập câu" };
          showToast(roundLabels[MIX_TYPES[_mixRound]], 2000);
        }
      } else {
        currentQIndex++;
      }
      const nq = currentQuestionsList[currentQIndex];
      if (nq) nq.isAnsweredCorrectly = false;
      saveAppState();
      renderExercise();
      await renderSidebar();
      if (exerciseMode === "listen" && nq) speakForMode(nq);
      return true;
    }
    async function movePrev() {
      if (!currentQuestionsList.length) return;
      currentQIndex =
        currentQIndex > 0
          ? currentQIndex - 1
          : currentQuestionsList.length - 1;
      const q = currentQuestionsList[currentQIndex];
      if (q) q.isAnsweredCorrectly = false;
      const inp = document.getElementById("dynamicAnswerInput");
      if (inp) {
        inp.value = "";
        inp.classList.remove("correct-border", "wrong-border");
        document
          .getElementById("exampleRevealBox")
          ?.classList.remove("visible");
      }
      renderExercise();
      await renderSidebar();
      saveAppState();
      if (exerciseMode === "listen" && q) speakForMode(q);
    }
    async function masterCurrentWord() {
      if (!currentQuestionsList.length) return;
      const spliceIdx = currentQIndex % currentQuestionsList.length;
      const item = currentQuestionsList[spliceIdx];
      if (!item) return;
      const sessId = item._sessId || currentSessionId,
        realId = item._realId || item.id;
      await dbMarkMastered(sessId, realId);
      // Đã thuộc thì không thể còn ở trạng thái "cần chú ý" nữa
      await dbUnmarkFlagged(sessId, realId);
      if (sessId === currentSessionId) {
        masteredIds.add(realId);
        flaggedIds.delete(realId);
      }
      item.isFlagged = false;
      _updateFlagUIInPlace(realId, false);
      _updateSidebarItemFlag(realId, false);

      // Khi học tất cả từ (kể cả đã thuộc): chỉ cập nhật trạng thái, giữ nguyên vị trí
      if (!onlyUnmastered) {
        item.isMastered = true;
        showToast(`✅ "${item.fullDisplayGerman}" — đã thuộc`, 2000);
        renderExercise();
        await renderSidebar();
        return;
      }

      const removed = item;
      // Xóa từ khỏi batch hiện tại
      currentQuestionsList.splice(spliceIdx, 1);

      // --- Chế độ batch: xóa từ khỏi batch, không bù thêm ---
      if (wordLimit > 0) {
        const fullList = await buildFullList();
        const sourceList =
          isCustomMode && window.customMaster?.length
            ? window.customMaster.filter(
              (i) => !masteredIds.has(i._realId || i.id),
            )
            : fullList;

        if (!sourceList.length) {
          // Đã thuộc hết toàn bộ
          showToast("🎉 Đã thuộc hết tất cả từ!", 3000);
          currentQuestionsList = [];
          stats = { totalAttempts: 0, correctCount: 0 };
          updateBatchBar(sourceList);
          saveAppState();
          renderExercise();
          await renderSidebar();
          return;
        }

        if (!currentQuestionsList.length) {
          // Batch hiện tại đã hết → tự động chuyển sang batch tiếp
          const totalBatches = Math.ceil(sourceList.length / wordLimit);
          let nextBatch = (window.batchIdx || 0) + 1;
          if (nextBatch >= totalBatches) nextBatch = 0;
          window.batchIdx = nextBatch;
          // Sang batch mới → luôn bắt đầu lại từ Lượt Nguyên từ (mixedRandom)
          if (currentExerciseType === "mixedRandom") _mixRound = 0;
          const s = nextBatch * wordLimit;
          // Dùng _shuffledOrder đã cố định (như goToBatch) để giữ thứ tự random
          let orderedSource = sourceList;
          if (randomMode && window._shuffledOrder?.length === sourceList.length) {
            const orderMap = new Map(window._shuffledOrder.map((id, i) => [id, i]));
            orderedSource = [...sourceList].sort(
              (a, b) => (orderMap.get(a.id) ?? 999) - (orderMap.get(b.id) ?? 999),
            );
          } else if (randomMode && window._shuffledOrder) {
            // sourceList nhỏ hơn (đã bỏ từ thuộc) — lọc _shuffledOrder còn lại
            const remaining = new Set(sourceList.map(i => i.id));
            const filteredOrder = window._shuffledOrder.filter(id => remaining.has(id));
            const orderMap = new Map(filteredOrder.map((id, i) => [id, i]));
            orderedSource = [...sourceList].sort(
              (a, b) => (orderMap.get(a.id) ?? 999) - (orderMap.get(b.id) ?? 999),
            );
          }
          let list = [...orderedSource].slice(s, s + wordLimit);
          currentQuestionsList = list;
          currentQuestionsList.forEach(
            (i) => (i.isAnsweredCorrectly = false),
          );
          currentQIndex = 0;
          stats = { totalAttempts: 0, correctCount: 0 };
          updateBatchBar(sourceList);
          saveAppState();
          renderExercise();
          await renderSidebar();
          showToast(
            `🎉 Xong batch! Chuyển sang batch ${nextBatch + 1}/${totalBatches}`,
            2500,
          );
          if (exerciseMode === "listen" && currentQuestionsList[0])
            speakForMode(currentQuestionsList[0]);
          return;
        }

        // Batch vẫn còn từ — tiếp tục với số từ đã giảm
        showToast(
          `✅ "${removed.fullDisplayGerman}" — còn ${currentQuestionsList.length} từ`,
          2000,
        );

        updateBatchBar(sourceList);
        if (currentQIndex >= currentQuestionsList.length) currentQIndex = 0;
        if (currentQuestionsList[currentQIndex])
          currentQuestionsList[currentQIndex].isAnsweredCorrectly = false;
        saveAppState();
        renderExercise();
        await renderSidebar();
        if (exerciseMode === "listen" && currentQuestionsList[currentQIndex])
          speakForMode(currentQuestionsList[currentQIndex]);
        return;
      }

      // --- Không có batch limit ---
      if (!currentQuestionsList.length) {
        showToast("🎉 Đã thuộc hết tất cả từ!", 3000);
        stats = { totalAttempts: 0, correctCount: 0 };
        renderExercise();
        await renderSidebar();
        return;
      }
      if (currentQIndex >= currentQuestionsList.length) currentQIndex = 0;
      saveAppState();
      if (currentQuestionsList[currentQIndex])
        currentQuestionsList[currentQIndex].isAnsweredCorrectly = false;
      showToast(
        `✅ "${removed.fullDisplayGerman}" — còn ${currentQuestionsList.length} từ`,
        2000,
      );
      renderExercise();
      await renderSidebar();
      if (exerciseMode === "listen" && currentQuestionsList[currentQIndex])
        speakForMode(currentQuestionsList[currentQIndex]);
    }
    async function deleteWord(id, sessId) {
      sessId = sessId || currentSessionId;
      showLoading("Đang xóa...");
      try {
        await dbDeleteWord(sessId, id);
        try { await dbUnmarkMastered(sessId, id); } catch (_) { }
        try { await dbUnmarkFlagged(sessId, id); } catch (_) { }
        masteredIds.delete(id);
        flaggedIds.delete(id);
        // In merged mode, q.id is "sessId:realId" so match both q.id and q._realId
        currentQuestionsList = currentQuestionsList.filter(
          (q) => q.id !== id && q._realId !== id,
        );
        if (window.customMaster)
          window.customMaster = window.customMaster.filter(
            (q) => q.id !== id && q._realId !== id,
          );
        if (window.selectedWordIds)
          window.selectedWordIds = window.selectedWordIds.filter((i) => i !== id);
        if (currentQIndex >= currentQuestionsList.length && currentQIndex > 0)
          currentQIndex--;
        await renderSessionDropdowns();
        renderExercise();
        await renderSidebar();
        showToast("🗑 Đã xóa từ");
      } catch (err) {
        console.error("deleteWord error:", err);
        if (err?.code === "permission-denied" || err?.message?.includes("permissions")) {
          showToast("❌ Không có quyền xóa. Vui lòng đăng nhập lại.");
        } else {
          showToast("❌ Lỗi khi xóa: " + (err?.message || err));
        }
      } finally {
        hideLoading();
      }
    }
    async function openEditWord(id, source, sessId) {
      let word;
      if (source === "session" || source === "merged") {
        const allWords = await buildFullListAll();
        word = allWords.find((v) => v.id === id || v._realId === id);
        if (word) word.originalGerman = word.fullDisplayGerman;
      } else {
        if (word) word.fullDisplayGerman = word.originalGerman;
      }
      if (!word) return;
      currentEditingWord = {
        id,
        sessId: sessId || currentSessionId,
        originalGerman: word.originalGerman || word.fullDisplayGerman,
        mainGerman: word.mainGerman || word.fullDisplayGerman,
      };
      currentEditingSource = source;
      document.getElementById("editGerman").value =
        currentEditingWord.originalGerman;
      document.getElementById("editWordType").value = word.wordType || "";
      document.getElementById("editMeaning").value = word.meaning;
      document.getElementById("editExample").value = word.example || "";
      openModal("editWordModal");
    }
    async function saveEditWord() {
      if (!currentEditingWord) return;
      const g = document.getElementById("editGerman").value.trim(),
        wt = document.getElementById("editWordType").value.trim(),
        m = document.getElementById("editMeaning").value.trim(),
        ex = document.getElementById("editExample").value.trim();
      if (!g || !m) {
        showToast("⚠️ Nhập đủ từ và nghĩa!");
        return;
      }
      let main = g.split("/")[0].trim();
      if (!main.match(/^(der|die|das)\s+\S+/)) main = g;
      const realId = currentEditingWord.id.includes(":")
        ? currentEditingWord.id.split(":").slice(1).join(":")
        : currentEditingWord.id;
      if (
        currentEditingSource === "session" ||
        currentEditingSource === "merged"
      ) {
        window._suppressRemoteReload = true;
        try {
          await dbUpdateWord(
            currentEditingWord.sessId || currentSessionId,
            realId,
            g,
            main,
            m,
            wt,
            ex,
          );
        } catch (err) {
          console.error("saveEditWord error:", err);
          window._suppressRemoteReload = false;
          showToast("❌ Lỗi lưu từ: " + (err?.message || err));
          return;
        }
        // Giữ flag trong 2s để chặn cả 2 lần onSnapshot (hasPendingWrites + committed)
        setTimeout(() => { window._suppressRemoteReload = false; }, 2000);
      }
      closeModal("editWordModal");

      // Cập nhật in-place trong currentQuestionsList — giữ nguyên thứ tự batch
      // Trong merged mode, item.id có dạng "sessId:realId" nên cần khớp cả _realId
      const targetId = currentEditingWord.id;
      const _applyEdit = (item) => {
        if (item.id === targetId || item._realId === targetId) {
          item.originalGerman = g;
          item.fullDisplayGerman = g;
          item.mainGerman = main;
          item.meaning = m;
          item.wordType = wt;
          item.example = ex;
        }
      };
      currentQuestionsList.forEach(_applyEdit);
      // Cập nhật cả customMaster nếu đang dùng
      if (window.customMaster) window.customMaster.forEach(_applyEdit);

      currentEditingWord = null;
      renderExercise();
      await renderSidebar();
      saveAppState();
      showToast("✅ Đã cập nhật từ");
      await syncCustomSelectionWithFilter();
    }
    async function openAddModal() {
      [
        "modalGerman",
        "modalWordType",
        "modalMeaning",
        "modalExample",
        "batchTextArea",
      ].forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = "";
      });
      document.getElementById("importStatusModal").textContent = "";
      ["manualForm", "batchForm", "importFormInModal"].forEach(
        (id) => (document.getElementById(id).style.display = "none"),
      );
      document.getElementById("manualForm").style.display = "";
      ["manualTabBtn", "batchTabBtn", "importTabBtn"].forEach((id) =>
        document.getElementById(id)?.classList.remove("active"),
      );
      document.getElementById("manualTabBtn").classList.add("active");
      openModal("addWordModal");
      setTimeout(() => document.getElementById("modalGerman").focus(), 320);
    }
    async function confirmAddManual() {
      const g = document.getElementById("modalGerman").value.trim(),
        wt = document.getElementById("modalWordType").value.trim(),
        m = document.getElementById("modalMeaning").value.trim(),
        ex = document.getElementById("modalExample").value.trim();
      if (!g || !m) {
        showToast("⚠️ Nhập đủ từ và nghĩa!");
        return;
      }
      let main = g.split("/")[0].trim();
      if (!main.match(/^(der|die|das)\s+\S+/)) main = g;
      const newId = uid();
      const newWord = {
        id: newId,
        originalGerman: g,
        mainGerman: main,
        meaning: m,
        wordType: wt,
        example: ex,
      };
      const cacheKey = `vocab_${currentSessionId}`;
      let vocab = [];
      try {
        const cached = localStorage.getItem(cacheKey);
        if (cached) vocab = JSON.parse(cached);
      } catch (e) { }
      const order = vocab.length;
      vocab.push({ ...newWord, sortOrder: order });
      localStorage.setItem(cacheKey, JSON.stringify(vocab));
      closeModal("addWordModal");
      await renderSessionDropdowns();
      await reloadPracticeList(false);
      showToast("✅ Đã thêm từ mới");
      dbAddWord(currentSessionId, newWord, order).catch((err) => {
        console.error("Firestore sync failed:", err);
        invalidateVocabCache(currentSessionId);
        showToast("⚠️ Lỗi lưu cloud — sẽ thử lại khi reload");
      });
    }
    async function confirmAddBatch() {
      const raw = document.getElementById("batchTextArea").value;
      if (!raw.trim()) return;
      const lines = raw.split(/\r?\n/);
      let count = 0;
      const vocab = await dbGetSessionVocab(currentSessionId);
      for (const line of lines) {
        const p = parseGermanLine(line);
        if (p) {
          await dbAddWord(
            currentSessionId,
            { id: uid(), ...p },
            vocab.length + count,
          );
          count++;
        }
      }
      closeModal("addWordModal");
      await renderSessionDropdowns();
      await reloadPracticeList(false);
      showToast(`✅ Đã thêm ${count} từ`);
    }
    async function importFromFile(file, statusEl, onDone) {
      if (!statusEl) statusEl = document.getElementById("importStatus");
      statusEl.textContent = "⏳ Đang nhập...";
      statusEl.style.color = "var(--text3)";
      const ext = file.name.split(".").pop().toLowerCase();
      async function doImportWords(words) {
        let count = 0;
        const vocab = await dbGetSessionVocab(currentSessionId);
        for (const item of words) {
          const g = (item.german || item.originalGerman || item.de || "")
            .toString()
            .trim();
          const wt = (item.wordType || item.word_type || item.type || "")
            .toString()
            .trim();
          const m = (item.meaning || item.vi || item.vietnamese || "")
            .toString()
            .trim();
          const ex = (item.example || item.beispiel || "").toString().trim();
          if (!g || !m) continue;
          let main = g.split("/")[0].trim();
          if (!main.match(/^(der|die|das)\s+\S+/)) main = g;
          await dbAddWord(
            currentSessionId,
            {
              id: uid(),
              originalGerman: g,
              mainGerman: main,
              meaning: m,
              wordType: wt,
              example: ex,
            },
            vocab.length + count,
          );
          count++;
        }
        return count;
      }
      if (ext === "json") {
        const reader = new FileReader();
        reader.onload = async (ev) => {
          try {
            const data = JSON.parse(ev.target.result);
            if (!Array.isArray(data)) {
              statusEl.textContent = "❌ File JSON không hợp lệ!";
              statusEl.style.color = "#f78166";
              return;
            }
            const count = await doImportWords(data);
            statusEl.textContent = `✅ Đã nhập ${count} từ!`;
            statusEl.style.color = "#3fb950";
            await renderSessionDropdowns();
            await reloadPracticeList(false);
            showToast(`✅ Nhập ${count} từ từ JSON`);
            if (onDone) onDone(count);
          } catch (e) {
            statusEl.textContent = "❌ Lỗi đọc file JSON!";
            statusEl.style.color = "#f78166";
          }
        };
        reader.readAsText(file, "utf-8");
      } else if (ext === "xlsx" || ext === "xls") {
        const reader = new FileReader();
        reader.onload = async (ev) => {
          try {
            const wb = XLSX.read(new Uint8Array(ev.target.result), {
              type: "array",
            });
            const ws = wb.Sheets[wb.SheetNames[0]];
            const rows = XLSX.utils.sheet_to_json(ws, { header: 1 });
            const startRow =
              rows[0] &&
                (rows[0][0] === "Từ tiếng Đức" ||
                  rows[0][0] === "German" ||
                  rows[0][0] === "german")
                ? 1
                : 0;
            const words = rows
              .slice(startRow)
              .map((row) => ({
                german: (row[0] || "").toString().trim(),
                wordType: (row[1] || "").toString().trim(),
                meaning: (row[2] || "").toString().trim(),
                example: (row[3] || "").toString().trim(),
              }))
              .filter((w) => w.german && w.meaning);
            const count = await doImportWords(words);
            statusEl.textContent = `✅ Đã nhập ${count} từ!`;
            statusEl.style.color = "#3fb950";
            await renderSessionDropdowns();
            await reloadPracticeList(false);
            showToast(`✅ Nhập ${count} từ từ Excel`);
            if (onDone) onDone(count);
          } catch (err) {
            statusEl.textContent = "❌ Lỗi đọc file Excel!";
            statusEl.style.color = "#f78166";
          }
        };
        reader.readAsArrayBuffer(file);
      } else {
        statusEl.textContent = "❌ Chỉ hỗ trợ JSON và Excel!";
        statusEl.style.color = "#f78166";
      }
    };
    window.closeModal = closeModal;
    window.openEditWord = openEditWord;

    async function openSelectWordsModal() {
      document.getElementById("selFilterRow")?.remove();
      const _actionsRow = document.querySelector(".wg-actions-row");
      _actionsRow.innerHTML = "";
      _actionsRow.style.display = "none";
      const allWords = await buildFullListAll();
      const selIds = new Set(
        window.selectedWordIds
          ? window.selectedWordIds
          : allWords.map((i) => i.id),
      );
      document.getElementById("selectWordsModal").dataset.mode = "global";
      const filterRow = document.createElement("div");
      filterRow.id = "selFilterRow";
      filterRow.style.cssText =
        "display:flex;flex-direction:column;gap:6px;margin-bottom:8px";
      filterRow.innerHTML = `<div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:6px"><input id="selSearchInput" placeholder="Tìm từ..." style="flex:1;min-width:100px;background:var(--select-bg);border:1px solid var(--border);color:var(--tx);font-family:inherit;font-size:.82rem;padding:5px 10px;border-radius:6px;outline:none;min-height:32px" type="text"><select id="selMasteredFilter" class="mselect" style="font-size:.78rem;padding:5px 6px;min-height:32px"><option value="all">Tất cả</option><option value="active" selected>Chưa thuộc</option><option value="mastered">Đã thuộc</option><option value="flagged">Chú ý</option></select><select id="selTypeFilter" class="mselect" style="font-size:.78rem;padding:5px 6px;min-height:32px"><option value="all">Tất cả</option><option value="n">Danh từ</option><option value="v">Động từ</option><option value="other">Khác</option></select></div>`;
      document
        .querySelector(".wg-actions-row")
        .parentNode.insertBefore(
          filterRow,
          document.querySelector(".wg-actions-row").nextSibling,
        );
      // Khôi phục tiêu chí lọc đã áp dụng lần trước (nếu có) thay vì luôn reset về "Tất cả"
      if (window.customFilterCriteria) {
        const { mv, tv, sv } = window.customFilterCriteria;
        if (mv) document.getElementById("selMasteredFilter").value = mv;
        if (tv) document.getElementById("selTypeFilter").value = tv;
        if (sv) document.getElementById("selSearchInput").value = sv;
      }
      function getFiltered() {
        const mv = document.getElementById("selMasteredFilter").value,
          tv = document.getElementById("selTypeFilter").value,
          sv = (document.getElementById("selSearchInput").value || "")
            .toLowerCase()
            .trim();
        let list = allWords;
        if (mv === "active") list = list.filter((v) => !v.isMastered);
        else if (mv === "mastered") list = list.filter((v) => v.isMastered);
        else if (mv === "flagged") list = list.filter((v) => v.isFlagged);
        if (tv === "n")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return wt === "n" || wt.startsWith("n ");
          });
        else if (tv === "v")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return wt === "v" || wt.startsWith("v ");
          });
        else if (tv === "other")
          list = list.filter((v) => {
            const wt = (v.wordType || "").toLowerCase();
            return (
              wt !== "n" &&
              !wt.startsWith("n ") &&
              wt !== "v" &&
              !wt.startsWith("v ")
            );
          });
        if (sv)
          list = list.filter(
            (v) =>
              v.fullDisplayGerman.toLowerCase().includes(sv) ||
              v.meaning.toLowerCase().includes(sv),
          );
        return list;
      }
      function selectByFilter() {
        selIds.clear();
        getFiltered().forEach((v) => selIds.add(v.id));
      }
      function renderGrid() {
        const filtered = getFiltered();
        document.getElementById("wordGrid").innerHTML = filtered
          .map(
            (item) =>
              `<label class="wg-item" style="${item.isMastered ? "background:rgba(63,185,80,.07);border-radius:6px" : item.isFlagged ? "background:rgba(240,192,0,.13);border-radius:6px" : ""}"><input type="checkbox" class="sel-cb" data-id="${item.id}"${selIds.has(item.id) ? " checked" : ""}><div><div class="wg-de font-semibold" style="color:var(--tx)">${escapeHtml(item.fullDisplayGerman)}${item.wordType ? `<span style="font-size:.62rem;background:rgba(210,168,255,.12);border:1px solid rgba(210,168,255,.25);color:#d2a8ff;padding:1px 5px;border-radius:4px;margin-left:5px">${escapeHtml(item.wordType)}</span>` : ""}${item.isFlagged ? `<span style="font-size:.62rem;background:rgba(240,192,0,.13);border:1px solid rgba(240,192,0,.35);color:#f0c000;padding:1px 5px;border-radius:4px;margin-left:3px"><i class="fa-solid fa-star"></i></span>` : ""}</div><div class="wg-vi text-[#6e7681] text-[0.72rem] font-mono">${escapeHtml(item.meaning)}</div>${item.isMastered ? '<div style="font-size:.66rem;color:#3fb950;font-weight:700;margin-top:1px"><i class="fa-solid fa-star"></i> đã thuộc</div>' : ""}</div></label>`,
          )
          .join("");
        updateSelCount(filtered.length);
        document.querySelectorAll("#wordGrid .sel-cb").forEach((cb) =>
          cb.addEventListener("change", () => {
            if (cb.checked) selIds.add(cb.dataset.id);
            else selIds.delete(cb.dataset.id);
            updateSelCount(getFiltered().length);
          }),
        );
      }
      // Bộ lọc giờ đóng vai trò chọn trực tiếp theo thời gian thực:
      // đổi bộ lọc → tự động chọn đúng tập từ khớp bộ lọc đó (không cần nút Chọn tất cả/Bỏ tất cả)
      function onFilterChange() {
        selectByFilter();
        renderGrid();
      }
      document.getElementById("selSearchInput").oninput = onFilterChange;
      document.getElementById("selMasteredFilter").onchange = onFilterChange;
      document.getElementById("selTypeFilter").onchange = onFilterChange;
      document.getElementById("applySelectModal").onclick = async () => {
        const filtered = getFiltered(),
          filteredIds = new Set(filtered.map((v) => v.id));
        const selected = [...selIds].filter((id) => filteredIds.has(id));
        if (!selected.length) {
          showToast("⚠️ Chọn ít nhất 1 từ!");
          return;
        }
        closeModal("selectWordsModal");
        const selectedWords = allWords
          .filter((i) => selected.includes(i.id))
          .map((i) => ({
            ...i,
            isAnsweredCorrectly: false,
          }));
        window.customMaster = selectedWords;
        window.selectedWordIds = selected;
        // Lưu tiêu chí lọc để tiếp tục tự động cập nhật danh sách đang học
        // khi trạng thái thuộc/chú ý/loại từ thay đổi ở nơi khác (VD: bỏ chú ý ngoài modal)
        window.customFilterCriteria = {
          mv: document.getElementById("selMasteredFilter").value,
          tv: document.getElementById("selTypeFilter").value,
          sv: (document.getElementById("selSearchInput").value || "").toLowerCase().trim(),
        };
        isCustomMode = true;
        window.batchIdx = 0;
        // Tạo thứ tự ngẫu nhiên CỐ ĐỊNH một lần cho toàn bộ list
        if (randomMode) {
          window._shuffledOrder = shuffleArray([...window.customMaster].map((i) => i.id));
        } else {
          window._shuffledOrder = null;
        }
        let orderedMaster = window.customMaster;
        if (randomMode && window._shuffledOrder) {
          const orderMap = new Map(window._shuffledOrder.map((id, i) => [id, i]));
          orderedMaster = [...window.customMaster].sort(
            (a, b) => (orderMap.get(a.id) ?? 999) - (orderMap.get(b.id) ?? 999),
          );
        }
        let list = [...orderedMaster];
        if (wordLimit > 0 && list.length > wordLimit)
          list = list.slice(0, wordLimit);
        currentQuestionsList = list;
        currentQuestionsList.forEach((i) => (i.isAnsweredCorrectly = false));
        currentQIndex = 0;
        stats = { totalAttempts: 0, correctCount: 0 };
        renderExercise();
        await renderSidebar();
        updateBatchBar(window.customMaster);
        saveAppState();
        showToast(`✅ Đang học ${window.customMaster.length} từ đã chọn`);
      };
      selectByFilter();
      renderGrid();
      openModal("selectWordsModal");
    }
    function openSelectInBatchModal() {
      document.getElementById("selFilterRow")?.remove();
      const _actionsRow = document.querySelector(".wg-actions-row");
      _actionsRow.style.display = "";
      _actionsRow.innerHTML =
        `<button class="wg-abtn" id="selAllBtn"><i class="fa-solid fa-circle-check"></i> Chọn tất cả</button><button class="wg-abtn" id="deselAllBtn"><i class="fa-solid fa-circle-xmark"></i> Bỏ tất cả</button>`;
      document.getElementById("selAllBtn").onclick = () => {
        document
          .querySelectorAll("#wordGrid .sel-cb")
          .forEach((cb) => (cb.checked = true));
        updateSelCount(document.querySelectorAll("#wordGrid .sel-cb").length);
      };
      document.getElementById("deselAllBtn").onclick = () => {
        document
          .querySelectorAll("#wordGrid .sel-cb")
          .forEach((cb) => (cb.checked = false));
        updateSelCount(document.querySelectorAll("#wordGrid .sel-cb").length);
      };
      const batchList = currentQuestionsList;
      document.getElementById("wordGrid").innerHTML = batchList
        .map(
          (item) =>
            `<label class="wg-item" style="${item.isFlagged ? "background:rgba(240,192,0,.13);border-radius:6px" : ""}"><input type="checkbox" class="sel-cb" data-id="${item.id}" checked><div><div class="wg-de font-semibold" style="color:var(--tx)">${escapeHtml(item.fullDisplayGerman)}${item.isFlagged ? `<span style="font-size:.62rem;color:#f0c000;margin-left:4px"><i class="fa-solid fa-star"></i></span>` : ""}</div><div class="wg-vi text-[#6e7681] text-[0.72rem] font-mono">${escapeHtml(item.meaning)}</div></div></label>`,
        )
        .join("");
      document.getElementById("selectWordsModal").dataset.mode = "batch";
      document.getElementById("selectWordsModal")._batchList = batchList;
      updateSelCount(batchList.length);
      document
        .querySelectorAll("#wordGrid .sel-cb")
        .forEach((cb) =>
          cb.addEventListener("change", () =>
            updateSelCount(batchList.length),
          ),
        );
      openModal("selectWordsModal");
    }
    function updateSelCount(total) {
      document.getElementById("selCount").textContent =
        `${document.querySelectorAll("#wordGrid .sel-cb:checked").length}/${total}`;
    }

    async function exportToExcel(words, filename) {
      const data = [
        ["Từ tiếng Đức", "Loại từ", "Nghĩa", "Ví dụ"],
        ...words.map((w) => [
          w.originalGerman || w.german || w.fullDisplayGerman || "",
          w.wordType || "",
          w.meaning || "",
          w.example || "",
        ]),
      ];
      const ws = XLSX.utils.aoa_to_sheet(data);
      ws["!cols"] = [{ wch: 35 }, { wch: 10 }, { wch: 30 }, { wch: 45 }];
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "Từ vựng");
      try {
        const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array" });
        const blob = new Blob([wbout], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
        await downloadBlob(blob, filename, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      } catch (e) {
        showToast("❌ Lỗi xuất Excel: " + e.message);
      }
    }
    async function openExportSessionsModal() {
      const sessions = await dbGetAllSessions();
      const list = document.getElementById("exportSessionsList");
      const items = await Promise.all(
        sessions.map(async (s) => {
          const vocab = await dbGetSessionVocab(s.id),
            mast = await dbGetMastered(s.id);
          return `<label class="sess-export-item"><input type="checkbox" class="export-sess-cb" data-id="${s.id}" checked><span class="sess-export-name flex-1 text-[0.84rem] font-semibold overflow-hidden text-ellipsis whitespace-nowrap" style="color:var(--tx)">${escapeHtml(s.name)}</span><span class="text-[0.72rem] font-mono whitespace-nowrap" style="color:var(--tx3)">${vocab.length} từ · ${mast.size} thuộc</span></label>`;
        }),
      );
      list.innerHTML = items.join("");
      updateExportSelCount();
      list
        .querySelectorAll(".export-sess-cb")
        .forEach((cb) => cb.addEventListener("change", updateExportSelCount));
      document.getElementById("exportSelAllBtn").onclick = () => {
        list
          .querySelectorAll(".export-sess-cb")
          .forEach((cb) => (cb.checked = true));
        updateExportSelCount();
      };
      document.getElementById("exportDeselAllBtn").onclick = () => {
        list
          .querySelectorAll(".export-sess-cb")
          .forEach((cb) => (cb.checked = false));
        updateExportSelCount();
      };
      closeModal("settingsModal");
      openModal("exportSessionsModal");
    }
    function updateExportSelCount() {
      const checked = document.querySelectorAll(
        "#exportSessionsList .export-sess-cb:checked",
      ).length,
        total = document.querySelectorAll(
          "#exportSessionsList .export-sess-cb",
        ).length;
      document.getElementById("exportSelCount").textContent =
        `${checked}/${total} phiên`;
    }
    let _importSessionsData = null;
    let _importCallerModal = null; // track which modal opened importSessionsModal
    async function openImportSessionsModal(callerModal) {
      _importCallerModal = callerModal || null;
      _importSessionsData = null;
      document.getElementById("importSessionsStatus").textContent = "";
      document.getElementById("importSessionsPreview").style.display = "none";
      document.getElementById("importSessionsPreviewList").innerHTML = "";
      document.getElementById("confirmImportSessionsBtn").disabled = true;
      document.getElementById("importSessionsFileInput").value = "";
      // Close whatever modal may be open
      ["settingsModal", "folderModal"].forEach(id => {
        const el = document.getElementById(id);
        if (el?.classList.contains("open")) closeModal(id);
      });
      openModal("importSessionsModal");
    }
    async function handleImportSessionsFile(file) {
      if (!file) return;
      const statusEl = document.getElementById("importSessionsStatus");
      statusEl.textContent = "⏳ Đang đọc file...";
      statusEl.style.color = "#6e7681";
      const reader = new FileReader();
      reader.onload = async (ev) => {
        try {
          const data = JSON.parse(ev.target.result);
          if (!data._format || !Array.isArray(data.sessions)) {
            statusEl.textContent = "❌ File không đúng định dạng.";
            statusEl.style.color = "#f78166";
            document.getElementById("confirmImportSessionsBtn").disabled =
              true;
            return;
          }
          _importSessionsData = data;
          const totalWords = data.sessions.reduce(
            (s, sess) => s + (sess.vocabulary?.length || 0),
            0,
          );
          statusEl.textContent = `✅ Đọc thành công: ${data.sessions.length} phiên, ${totalWords} từ`;
          statusEl.style.color = "#3fb950";
          const allSessions = await dbGetAllSessions();
          document.getElementById("importSessionsPreviewList").innerHTML =
            data.sessions
              .map((sess) => {
                const mc = (sess.vocabulary || []).filter(
                  (w) => w.mastered,
                ).length,
                  fc = (sess.vocabulary || []).filter(
                    (w) => w.flagged,
                  ).length;
                const ex = allSessions.find((s) => s.name === sess.name);
                const badge = ex
                  ? `<span style="font-size:.65rem;background:rgba(88,166,255,.12);border:1px solid rgba(88,166,255,.3);color:#58a6ff;padding:1px 6px;border-radius:8px;margin-left:5px">+thêm vào</span>`
                  : `<span style="font-size:.65rem;background:rgba(63,185,80,.12);border:1px solid rgba(63,185,80,.3);color:#3fb950;padding:1px 6px;border-radius:8px;margin-left:5px">mới</span>`;
                return `<div style="display:flex;align-items:center;gap:8px;padding:7px 8px;border-radius:6px;background:var(--bg3);margin-bottom:3px"><div style="flex:1;min-width:0"><div style="font-size:.84rem;font-weight:600;color:var(--tx)">${escapeHtml(sess.name)}${badge}</div><div style="font-size:.72rem;color:var(--tx3);font-family:DM Mono,monospace">${(sess.vocabulary || []).length} từ · ${mc} thuộc · ${fc} <i class="fa-solid fa-star"></i></div></div></div>`;
              })
              .join("");
          document.getElementById("importSessionsPreview").style.display =
            "block";
          document.getElementById("confirmImportSessionsBtn").disabled =
            false;
        } catch (e) {
          statusEl.textContent = "❌ File JSON không hợp lệ!";
          statusEl.style.color = "#f78166";
          document.getElementById("confirmImportSessionsBtn").disabled = true;
        }
      };
      reader.readAsText(file, "utf-8");
    }
    function openMobileSidebar() {
      document.getElementById("sidebar").classList.add("mobile-open");
      const ov = document.getElementById("mobileOverlay");
      ov.style.display = "block";
      requestAnimationFrame(() => ov.classList.add("visible"));
      const mb = document.getElementById("mobBatchInfo");
      if (mb) mb.style.zIndex = "100";
    }
    function closeMobileSidebar() {
      document.getElementById("sidebar").classList.remove("mobile-open");
      const ov = document.getElementById("mobileOverlay");
      ov.classList.remove("visible");
      setTimeout(() => (ov.style.display = "none"), 260);
      const mb = document.getElementById("mobBatchInfo");
      if (mb) mb.style.zIndex = "400";
    }
    function updateBatchBar(sourceList) {
      const bar = document.getElementById("batchbar"),
        mobInfo = document.getElementById("mobBatchInfo");
      if (!wordLimit) {
        bar?.classList.add("hidden");
        if (mobInfo) mobInfo.style.display = "none";
        return;
      }
      bar?.classList.remove("hidden");
      const total = Math.ceil(sourceList.length / wordLimit),
        cur = window.batchIdx || 0;
      document.getElementById("batchLabel").textContent =
        `${cur + 1}/${total}`;
      document.getElementById("prevBatchBtn").disabled = cur <= 0;
      document.getElementById("nextBatchBtn").disabled = cur + 1 >= total;
      if (mobInfo) {
        mobInfo.style.display = "flex";
        document.getElementById("mobBatchLabel").textContent =
          `${cur + 1}/${total}`;
        document.getElementById("mobPrevBatchBtn").disabled = cur <= 0;
        document.getElementById("mobNextBatchBtn").disabled =
          cur + 1 >= total;
      }
    }
    async function goToBatch(dir) {
      if (!wordLimit) return;
      const fullList = await buildFullList(),
        sourceList =
          isCustomMode && window.customMaster?.length
            ? window.customMaster
            : fullList;
      const total = Math.ceil(sourceList.length / wordLimit);
      let idx = window.batchIdx || 0;
      if (dir === "next") idx = idx + 1 >= total ? 0 : idx + 1;
      else idx = idx - 1 < 0 ? total - 1 : idx - 1;
      window.batchIdx = idx;
      // Sang batch mới → luôn bắt đầu lại từ Lượt Nguyên từ (mixedRandom)
      if (currentExerciseType === "mixedRandom") _mixRound = 0;
      const s = idx * wordLimit;
      // Dùng thứ tự đã cố định (_shuffledOrder) thay vì shuffle lại từng batch.
      // Không yêu cầu độ dài khớp tuyệt đối — chỉ cần lọc lại _shuffledOrder theo các id
      // còn tồn tại trong sourceList (từ mới/vừa đổi trạng thái sẽ nối vào cuối), để tránh
      // việc thứ tự random bị "reset" về thứ tự gốc mỗi khi flag/sửa từ làm đổi độ dài danh sách.
      let orderedSource = sourceList;
      if (randomMode && window._shuffledOrder?.length) {
        const remaining = new Set(sourceList.map((i) => i.id));
        const filteredOrder = window._shuffledOrder.filter((id) => remaining.has(id));
        const orderMap = new Map(filteredOrder.map((id, i) => [id, i]));
        orderedSource = [...sourceList].sort(
          (a, b) => (orderMap.get(a.id) ?? 999) - (orderMap.get(b.id) ?? 999),
        );
      }
      let list = [...orderedSource].slice(s, s + wordLimit);
      currentQuestionsList = list;
      currentQuestionsList.forEach((i) => (i.isAnsweredCorrectly = false));
      currentQIndex = 0;
      stats = { totalAttempts: 0, correctCount: 0 };
      saveAppState();
      updateBatchBar(sourceList);
      renderExercise();
      await renderSidebar();
      showToast(
        `Batch ${idx + 1}/${Math.ceil(sourceList.length / wordLimit)} — ${list.length} từ`,
        1500,
      );
      if (exerciseMode === "listen" && list[0]) speakForMode(list[0]);
    }

    function renderExercise() {
      const card = document.getElementById("exCard");
      if (!card) return;
      // Skip focus flag
      const _skipFocus = window._noFocusNext;
      window._noFocusNext = false;

      updateStatsBar();
      const total = currentQuestionsList.length,
        q = total ? currentQuestionsList[currentQIndex % total] : null;
      if (!total || !q) {
        card.innerHTML = `<div class="text-center py-9 px-4" style="color:var(--tx3)"><div class="text-[2.6rem] mb-2"><i class="fa-regular fa-folder-open"></i></div><p class="text-[0.92rem] leading-relaxed">Chưa có từ vựng.<br>Thêm từ vào phiên để bắt đầu!</p></div>`;
        return;
      }
      const pct = Math.round((currentQIndex / total) * 100);
      const wtBadge = q.wordType
        ? `<span style="font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;background:rgba(210,168,255,.12);border:1px solid rgba(210,168,255,.25);color:#d2a8ff;padding:2px 7px;border-radius:4px;margin-left:8px;vertical-align:middle;font-family:'DM Mono',monospace">${escapeHtml(q.wordType)}</span>`
        : "";
      const flagIndicatorHtml = q.isFlagged
        ? `<span class="flag-indicator" title="Cần chú ý"><i class="fa-solid fa-star"></i></span>`
        : "";
      const promptStateClass = _getPromptStateClass(q);

      // Build prompt tuỳ mode
      let typeTag = "",
        prompt = "";
      const effType = getEffectiveType(q);
      if (exerciseMode === "listen") {
        const replayBtn = `<span id="listenPlayBtn" style="font-size:2rem;cursor:pointer;user-select:none;flex-shrink:0;line-height:1;" title="Nghe lại"><i class="fa-solid fa-volume-high"></i></span>`;
        if (effType === "fullWord") {
          typeTag = "Nghe từ → Viết nghĩa";
          // Không hiển thị từ vựng trong ex-prompt nữa
          prompt = `${replayBtn}<span style="flex:1;min-width:0;font-size:1rem;color:var(--tx3);font-style:italic;opacity:0.6;">Nghe và viết nghĩa tiếng Việt…</span>${wtBadge}`;
        } else if (effType === "fullMeaning") {
          typeTag = "Nghe nghĩa → Viết từ";
          prompt = `${replayBtn}<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:1.15rem;color:var(--tx2);font-family:'DM Mono',monospace;opacity:0.8;">${escapeHtml(q.meaning)}</span>${wtBadge}`;
        } else if (effType === "fullSentence") {
          typeTag = "Nghe câu ví dụ → Viết lại";
          const _exMeaning = getVietnameseExample(q.example) || q.meaning;
          prompt = `${replayBtn}<div style="flex:1;min-width:0;"><div style="font-size:1.15rem;color:var(--tx2);font-family:'DM Mono',monospace;opacity:0.8;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escapeHtml(q.mainGerman || q.fullDisplayGerman)}</div><div style="font-size:0.85rem;color:var(--tx3);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">"${escapeHtml(_exMeaning)}"</div></div>${wtBadge}`;
        }
      } else if (effType === "fullWord") {
        typeTag = currentExerciseType === "mixedRandom" ? "Hỗn hợp · Lượt Nguyên từ" : "Nhập nguyên từ";
        prompt = `<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">"${escapeHtml(q.meaning)}"</span>${wtBadge}`;
      } else if (effType === "fullMeaning") {
        typeTag = currentExerciseType === "mixedRandom" ? "Hỗn hợp · Lượt Nghĩa" : "Nhập nghĩa";
        prompt = `<span style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">"${escapeHtml(q.fullDisplayGerman)}"</span>${wtBadge}`;
      } else if (effType === "fullSentence") {
        if (exerciseMode === "choose") {
          typeTag = currentExerciseType === "mixedRandom" ? "Hỗn hợp · Lượt Chọn câu" : "Chọn câu ví dụ";
        } else {
          typeTag = currentExerciseType === "mixedRandom" ? "Hỗn hợp · Lượt Nhập câu" : "Nhập câu ví dụ";
        }
        const _noExWarn = !q.example
          ? `<div style="font-size:0.74rem;color:#f0c000;margin-top:4px;"><i class="fa-solid fa-triangle-exclamation"></i> Từ này chưa có câu ví dụ mẫu</div>`
          : "";
        const _exMeaning = getVietnameseExample(q.example) || q.meaning;
        prompt = `<div style="flex:1;min-width:0;"><div style="font-size:0.95rem;color:var(--tx3);font-family:'DM Mono',monospace;margin-bottom:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${escapeHtml(q.fullDisplayGerman)}</div><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">"${escapeHtml(_exMeaning)}"</div></div>${wtBadge}${_noExWarn}`;
      }

      const sourceLabel =
        currentSource === "merged"
          ? "Gộp phiên"
          : currentSource === "session"
            ? "Phiên"
            : "Từ điển";
      const promptClickable = `cursor:pointer;user-select:none;`;
      const flagBtnHtml = q.isFlagged
        ? `<button class="exbtn unflag-btn" id="flagWordBtn"><i class="fa-regular fa-star"></i> ${isMobileView() ? "Bỏ Flag" : "Bỏ chú ý"}</button>`
        : `<button class="exbtn flag-btn" id="flagWordBtn"><i class="fa-solid fa-star"></i> ${isMobileView() ? "Flag" : "Chú ý"}</button>`;
      // Settings bar với 3 nút mode
      const _sbStyle = "background:transparent;border:1px solid var(--border);border-radius:5px;padding:4px 7px;cursor:pointer;font-size:0.75rem;line-height:1.6;touch-action:manipulation;user-select:none;position:relative;z-index:10;";
      const settingsBarHtml = `<div style="display:flex;gap:3px;flex-wrap:nowrap;flex-shrink:0;align-items:center;position:relative;z-index:10;">
        <button id="sbRandom" title="Random" style="${_sbStyle}opacity:${randomMode ? 1 : 0.4}"><i class="fa-solid fa-shuffle"></i></button>
        <button id="sbAutoAdv" title="Auto-next" style="${_sbStyle}opacity:${autoAdvanceOnCorrect ? 1 : 0.4}"><i class="fa-solid fa-bolt"></i></button>
        <button id="sbAllowSkip" title="Cho phép bỏ qua câu sai" style="${_sbStyle}opacity:${allowSkip ? 1 : 0.4}"><i class="fa-solid fa-forward-step"></i></button>
        <button id="sbSound" title="Âm thanh" style="${_sbStyle}opacity:${soundEnabled ? 1 : 0.4}"><i class="fa-solid fa-volume-high"></i></button>
        <button id="sbStudy" title="Học bài" style="${_sbStyle}opacity:${studyMode ? 1 : 0.4}"><i class="fa-solid fa-book-open"></i></button>
      </div>`;
      const useChoose =
        exerciseMode === "choose" &&
        (effType !== "fullSentence" || !!q.example);

      const masterBtnHtml = q.isMastered
        ? `<button id="masterBtn" title="Bỏ thuộc" style="position:absolute;top:10px;right:10px;background:rgba(63,185,80,0.15);border:1px solid rgba(63,185,80,0.45);color:#3fb950;border-radius:6px;padding:3px 10px;font-size:0.72rem;font-weight:700;cursor:pointer;font-family:'DM Mono',monospace;letter-spacing:.04em;z-index:2;white-space:nowrap"><i class="fa-solid fa-check"></i> Thuộc</button>`
        : `<button id="masterBtn" title="Đánh dấu thuộc" style="position:absolute;top:10px;right:10px;background:rgba(88,166,255,0.08);border:1px solid rgba(88,166,255,0.25);color:#8b949e;border-radius:6px;padding:3px 10px;font-size:0.72rem;font-weight:700;cursor:pointer;font-family:'DM Mono',monospace;letter-spacing:.04em;z-index:2;white-space:nowrap">Thuộc</button>`;

      if (useChoose) {
        const allList = currentQuestionsList;
        const choices = generateChoices(q, allList);
        const letters = ["A", "B", "C", "D"];
        const optionsHtml = choices
          .map(
            (c, i) =>
              `<button class="mc-option" data-correct="${c.isCorrect}" data-val="${escapeHtml(c.text)}"><span class="mc-option-letter w-6 h-6 rounded-full flex items-center justify-center text-[0.72rem] font-bold flex-shrink-0 font-syne">${letters[i]}</span><span>${escapeHtml(c.text)}</span></button>`,
          )
          .join("");
        card.innerHTML = `<div class="w-full h-[3px] bg-[#1c2333] rounded-sm overflow-hidden"><div class="progress-bar-fill" style="width:${pct}%"></div></div><div class="ex-prompt ${promptStateClass}" id="exPromptBox" style="position:relative;${promptClickable}"><div class="ex-type-row text-[0.68rem] font-bold uppercase tracking-widest mb-2 flex items-center gap-1 flex-wrap" style="color:var(--tx3);padding-right:80px;"><span class="w-[18px] h-[2px] bg-[#58a6ff] rounded-sm block flex-shrink-0"></span>${typeTag}${flagIndicatorHtml}</div><div class="text-[1.5rem] font-bold leading-snug mb-1 ex-question" style="color:var(--tx)">${prompt}</div><div style="display:flex;align-items:center;justify-content:space-between;gap:8px;"><div class="text-[0.8rem] font-mono" style="color:var(--tx3)">${currentQIndex + 1} / ${total} · ${sourceLabel}</div>${settingsBarHtml}</div>${masterBtnHtml}</div><div class="mc-options" id="mcOptions">${optionsHtml}</div><div class="flex gap-2 flex-wrap">${flagBtnHtml}</div><div class="shortcuts-box"><kbd>Ctrl</kbd>+<kbd>Alt</kbd> Nghe lại &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>⌫</kbd> Thuộc &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>F</kbd> Flag &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>\</kbd> Đổi dạng</div>`;

        const _promptSpeakHandler = (e) => {
          // Bỏ qua nếu bấm vào button hoặc bất kỳ element con của button
          if (e.target.closest("button")) return;
          e.preventDefault();
          unlockTTS();
          speakForMode(q);
          const inp = document.getElementById("dynamicAnswerInput");
          if (inp) inp.focus({ preventScroll: true });
        };
        document
          .getElementById("exPromptBox")
          ?.addEventListener("click", _promptSpeakHandler);
        document
          .getElementById("listenPlayBtn")
          ?.addEventListener("click", (e) => {
            e.stopPropagation();
            unlockTTS();
            speakForMode(q);
          });
        document
          .getElementById("masterBtn")
          ?.addEventListener("mousedown", (e) => e.preventDefault());
        document
          .getElementById("masterBtn")
          ?.addEventListener("click", (e) => {
            e.stopPropagation();
            if (q.isMastered) {
              const sessId = q._sessId || currentSessionId;
              const realId = q._realId || q.id;
              unmarkWordMasteredInSess(realId, sessId);
            } else {
              masterCurrentWord();
            }
          });
        document
          .getElementById("flagWordBtn")
          ?.addEventListener("click", () => toggleFlagCurrentWord());
        _bindSettingsBar();

        document.querySelectorAll(".mc-option").forEach((btn) => {
          const _mcHandler = () => {
            if (btn.classList.contains("disabled")) return;
            const isCorrect = btn.dataset.correct === "true";
            document.querySelectorAll(".mc-option").forEach((b) => {
              b.classList.add("disabled");
              if (b.dataset.correct === "true") b.classList.add("correct");
            });
            if (!isCorrect) btn.classList.add("wrong");
            if (!q.isAnsweredCorrectly) {
              if (isCorrect) {
                q.isAnsweredCorrectly = true;
                stats.correctCount++;
              }
              stats.totalAttempts++;
              updateStatsBar();
              // Không phát âm khi listen mode
              if (soundEnabled && isCorrect && exerciseMode !== "listen")
                speakText(q.fullDisplayGerman);
              if (isCorrect) {
                const p = document.querySelector(".ex-prompt");
                if (p && !q.isFlagged) {
                  p.classList.remove("state-normal");
                  p.classList.add("state-correct");
                }
              }
            }
            isWaitingForAutoNext = true;
            setTimeout(
              () => {
                isWaitingForAutoNext = false;
                moveNext(null, false, true);
              },
              isCorrect ? 600 : 1200,
            );
          };
          btn.addEventListener("touchend", (e) => { e.preventDefault(); _mcHandler(); });
          btn.addEventListener("click", _mcHandler);
        });

        // Auto-play listen mode (choose)
        if (exerciseMode === "listen") speakForMode(q);
      } else {
        // Write mode
        let placeholder = "";
        const _effType = getEffectiveType(q);
        if (_effType === "fullWord")
          placeholder =
            exerciseMode === "listen"
              ? "Viết nghĩa tiếng Việt..."
              : "der Tisch / laufen...";
        else if (_effType === "fullMeaning")
          placeholder =
            exerciseMode === "listen"
              ? "Viết lại từ tiếng Đức..."
              : "Nghĩa tiếng Việt...";
        else if (_effType === "fullSentence")
          placeholder =
            exerciseMode === "listen"
              ? "Viết lại câu vừa nghe..."
              : "Nhập một câu ví dụ chứa từ này...";
        else placeholder = "Nhập chính xác...";

        const btnsHtml = `
            <button class="exbtn" id="hintBtn"><i class="fa-solid fa-lightbulb"></i> Gợi ý</button>
            ${flagBtnHtml}
            <button class="exbtn" id="editWordBtn" style="border-color:rgba(210,168,255,.4);color:#d2a8ff;white-space:nowrap"><i class="fa-solid fa-pen"></i> Sửa</button>
            <button class="exbtn" id="exPrevBtn" style="${isMobileView() ? "" : "display:none"};border-color:rgba(88,166,255,.3);color:#58a6ff;padding:7px 16px;flex-shrink:0;margin-left:auto"><i class="fa-solid fa-backward-step"></i></button>
            <button class="exbtn" id="exNextBtn" style="${isMobileView() ? "" : "display:none"};border-color:rgba(88,166,255,.3);color:#58a6ff;padding:7px 16px;flex-shrink:0"><i class="fa-solid fa-forward-step"></i></button>
          `;

        card.innerHTML = `<div class="w-full h-[3px] bg-[#1c2333] rounded-sm overflow-hidden"><div class="progress-bar-fill" style="width:${pct}%"></div></div><div class="ex-prompt ${promptStateClass}" id="exPromptBox" style="position:relative;${promptClickable}"><div class="ex-type-row text-[0.68rem] font-bold uppercase tracking-widest mb-2 flex items-center gap-1 flex-wrap" style="color:var(--tx3);padding-right:80px;"><span class="w-[18px] h-[2px] bg-[#58a6ff] rounded-sm block flex-shrink-0"></span>${typeTag}${flagIndicatorHtml}</div><div class="ex-question text-[1.5rem] font-bold leading-snug mb-1" style="color:var(--tx)">${prompt}</div><div style="display:flex;align-items:center;justify-content:space-between;gap:8px;"><div class="text-[0.8rem] font-mono" style="color:var(--tx3)">${currentQIndex + 1} / ${total} · ${sourceLabel}</div>${settingsBarHtml}</div>${masterBtnHtml}</div><div><input id="dynamicAnswerInput" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false" placeholder="${placeholder}" inputmode="text"></div><div class="flex gap-2" id="exBtnsRow" style="flex-wrap:nowrap;gap:5px">${btnsHtml}</div><div class="example-box" id="exampleRevealBox"></div><div class="char-hint-wrap" id="charHintBox"></div><div class="shortcuts-box"><kbd>Ctrl</kbd>+<kbd>Alt</kbd> Nghe lại &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>[</kbd> Gợi ý &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>⌫</kbd> Thuộc &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>F</kbd> Flag &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>;</kbd> Trước &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>Space</kbd> Bỏ qua &nbsp;|&nbsp; <kbd>Ctrl</kbd>+<kbd>\\</kbd> Đổi dạng</div>`;


        const _promptSpeakHandler = (e) => {
          // Bỏ qua nếu bấm vào button hoặc bất kỳ element con của button
          if (e.target.closest("button")) return;
          e.preventDefault();
          unlockTTS();
          speakForMode(q);
          const inp = document.getElementById("dynamicAnswerInput");
          if (inp) inp.focus({ preventScroll: true });
        };
        document
          .getElementById("exPromptBox")
          ?.addEventListener("click", _promptSpeakHandler);
        document
          .getElementById("listenPlayBtn")
          ?.addEventListener("click", (e) => {
            e.stopPropagation();
            unlockTTS();
            speakForMode(q);
          });

        const input = document.getElementById("dynamicAnswerInput");
        if (input) {
          if (!_skipFocus) input.focus();
          input.addEventListener("input", (e) =>
            onInputChange(e.target.value, input),
          );
          input.addEventListener("keydown", (e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              moveNext(input, true);
              setTimeout(
                () => document.getElementById("dynamicAnswerInput")?.focus(),
                150,
              );
            }
          });
        }

        document
          .getElementById("hintBtn")
          ?.addEventListener("mousedown", (e) => e.preventDefault());
        document.getElementById("hintBtn")?.addEventListener("click", () => {
          const hb = document.getElementById("charHintBox");
          if (!hb) return;
          const isVis = hb.classList.contains("visible");
          if (isVis) hb.classList.remove("visible");
          else {
            // Chế độ nhập câu: luôn gợi ý câu ví dụ mẫu, không phụ thuộc nghe/viết
            if (getEffectiveType(q) === "fullSentence") {
              hb.innerHTML = q.example
                ? `<span style="color:var(--tx3);font-size:0.8rem;">Câu ví dụ: </span><span style="font-family:'DM Mono',monospace;color:var(--tx);">${escapeHtml(q.example)}</span>`
                : `<span style="color:var(--tx3);font-size:0.8rem;">Chưa có câu mẫu — câu phải chứa từ: </span><span style="font-family:'DM Mono',monospace;color:#58a6ff;font-weight:700;">${escapeHtml(q.mainGerman || q.fullDisplayGerman)}</span>`;
            } else if (exerciseMode === "listen") {
              // Listen mode: luôn hiển thị "từ tiếng Đức — nghĩa tiếng Việt", kèm câu ví dụ (nếu có)
              const _exampleLine = q.example
                ? `<div style="margin-top:6px;"><span style="color:var(--tx3);font-size:0.8rem;">Câu ví dụ: </span><span style="font-family:'DM Mono',monospace;color:var(--tx);">${escapeHtml(q.example)}</span></div>`
                : "";
              hb.innerHTML = `<div><span style="font-family:'DM Mono',monospace;color:var(--tx);font-weight:600">${escapeHtml(q.fullDisplayGerman)}</span><span style="color:var(--tx3);margin:0 6px">—</span><span style="color:var(--tx2)">${escapeHtml(q.meaning)}</span></div>${_exampleLine}`;
            } else {
              const inp = document.getElementById("dynamicAnswerInput"),
                uv = inp ? inp.value : "";
              // Hint target tuỳ mode
              const _et = getEffectiveType(q);
              const target =
                (_et === "fullMeaning" &&
                  exerciseMode !== "listen") ||
                  (_et === "fullWord" &&
                    exerciseMode === "listen")
                  ? q.meaning
                  : q.fullDisplayGerman;
              hb.innerHTML = buildCharHint(uv, target);
            }
            hb.classList.add("visible");
          }
          setTimeout(
            () => document.getElementById("dynamicAnswerInput")?.focus(),
            50,
          );
        });

        document
          .getElementById("masterBtn")
          ?.addEventListener("mousedown", (e) => e.preventDefault());
        document
          .getElementById("masterBtn")
          ?.addEventListener("click", () => {
            if (q.isMastered) {
              const sessId = q._sessId || currentSessionId;
              const realId = q._realId || q.id;
              unmarkWordMasteredInSess(realId, sessId);
            } else {
              masterCurrentWord();
            }
          });

        document
          .getElementById("editWordBtn")
          ?.addEventListener("mousedown", (e) => e.preventDefault());
        document
          .getElementById("editWordBtn")
          ?.addEventListener("click", () => {
            const realId = q._realId || q.id;
            const sessId = q._sessId || currentSessionId;
            openEditWord(realId, currentSource, sessId);
          });

        document
          .getElementById("flagWordBtn")
          ?.addEventListener("mousedown", (e) => e.preventDefault());
        document
          .getElementById("flagWordBtn")
          ?.addEventListener("click", () => {
            toggleFlagCurrentWord();
            setTimeout(
              () => document.getElementById("dynamicAnswerInput")?.focus(),
              50,
            );
          });

      }

      document
        .getElementById("exPrevBtn")
        ?.addEventListener("mousedown", (e) => e.preventDefault());
      document
        .getElementById("exPrevBtn")
        ?.addEventListener("click", () => movePrev());

      document
        .getElementById("exNextBtn")
        ?.addEventListener("mousedown", (e) => e.preventDefault());
      document
        .getElementById("exNextBtn")
        ?.addEventListener("click", () =>
          moveNext(
            document.getElementById("dynamicAnswerInput"),
            false,
            true,
          ),
        );

      _bindSettingsBar();

      // Auto-play khi listen mode (write)
      if (exerciseMode === "listen") speakForMode(q);
    }

    function _bindSettingsBar() {
      function _addBtn(el, handler) {
        if (!el) return;
        el.addEventListener("mousedown", (e) => e.preventDefault());
        el.addEventListener("touchend", (e) => { e.preventDefault(); e.stopPropagation(); handler(); });
        el.addEventListener("click", (e) => { e.stopPropagation(); handler(); });
      }
      ["sbRandom", "sbAutoAdv", "sbAllowSkip", "sbSound", "sbStudy"].forEach(
        (id) => {
          _addBtn(document.getElementById(id), () => {
            if (id === "sbRandom") {
              randomMode = !randomMode;
              localStorage.setItem("randomMode", randomMode);
              window._shuffledOrder = null;
            } else if (id === "sbAutoAdv") {
              autoAdvanceOnCorrect = !autoAdvanceOnCorrect;
              localStorage.setItem(
                "autoAdvanceOnCorrect",
                autoAdvanceOnCorrect,
              );
            } else if (id === "sbAllowSkip") {
              allowSkip = !allowSkip;
              localStorage.setItem("allowSkip", allowSkip);
              showToast(allowSkip ? "⏭ Bỏ qua: BẬT" : "🔒 Bỏ qua: TẮT");
            } else if (id === "sbSound") {
              soundEnabled = !soundEnabled;
              localStorage.setItem("soundEnabled", soundEnabled);
            } else if (id === "sbStudy") {
              studyMode = !studyMode;
              localStorage.setItem("studyMode", studyMode);
              if (studyMode) {
                autoAdvanceOnCorrect = false;
                localStorage.setItem("autoAdvanceOnCorrect", false);
              }
            }
            updateAllToggles();
            renderExercise();
          });
        },
      );

      // 3 nút mode trong quick-bar
      ["sbModeWrite", "sbModeChoose", "sbModeListen"].forEach((id) => {
        _addBtn(document.getElementById(id), () => {
          exerciseMode =
            id === "sbModeWrite"
              ? "write"
              : id === "sbModeChoose"
                ? "choose"
                : "listen";
          localStorage.setItem("exerciseMode", exerciseMode);
          updateAllToggles();
          renderExercise();
          showToast(
            exerciseMode === "choose"
              ? "Chọn"
              : exerciseMode === "listen"
                ? "Nghe"
                : "Viết",
          );
          focusAnswerInput();
        });
      });
    }

    function focusAnswerInput() {
      setTimeout(() => {
        const inp = document.getElementById("dynamicAnswerInput");
        if (inp) inp.focus();
      }, 80);
    }

    function initKeyboard() {
      document.addEventListener("keydown", (e) => {
        const inputFocused =
          document.activeElement?.id === "dynamicAnswerInput";
        if (e.ctrlKey && e.code === "Slash") {
          e.preventDefault();
          autoAdvanceOnCorrect = !autoAdvanceOnCorrect;
          localStorage.setItem("autoAdvanceOnCorrect", autoAdvanceOnCorrect);
          updateAllToggles();
          showToast(
            autoAdvanceOnCorrect ? "⚡ Auto-next: BẬT" : "⏸️ Auto-next: TẮT",
          );
          return;
        }
        if (e.ctrlKey && e.key === "g") {
          e.preventDefault();
          studyMode = !studyMode;
          localStorage.setItem("studyMode", studyMode);
          if (studyMode) {
            autoAdvanceOnCorrect = false;
            localStorage.setItem("autoAdvanceOnCorrect", false);
          }
          updateAllToggles();
          showToast(studyMode ? "Học bài: BẬT" : "Học bài: TẮT", 2200);
          return;
        }
        if (e.ctrlKey && e.key === "f" && inputFocused) {
          e.preventDefault();
          toggleFlagCurrentWord();
          return;
        }
        const anyModalOpen = document.querySelector(".modal-overlay.open");
        if (anyModalOpen) return;
        if (e.ctrlKey && e.code === "BracketLeft") {
          e.preventDefault();
          document.getElementById("hintBtn")?.click();
        } else if (e.ctrlKey && e.code === "BracketRight") {
          e.preventDefault();
          const q =
            currentQuestionsList[currentQIndex % currentQuestionsList.length];
        } else if (e.ctrlKey && e.code === "Period") {
          e.preventDefault();
          goToBatch("next");
        } else if (e.ctrlKey && e.code === "Comma") {
          e.preventDefault();
          goToBatch("prev");
        } else if (e.ctrlKey && e.code === "Quote") {
          e.preventDefault();
          moveNext(
            document.getElementById("dynamicAnswerInput"),
            false,
            true,
          );
        } else if (e.ctrlKey && e.code === "Semicolon") {
          e.preventDefault();
          movePrev();
        } else if (e.ctrlKey && e.code === "Backslash") {
          e.preventDefault();
          const types = ["fullWord", "fullMeaning", "fullSentence"];
          const labels = {
            fullWord: "📝 Nguyên từ",
            fullMeaning: "💬 Nghĩa",
            fullSentence: "Câu",
          };
          const next =
            types[(types.indexOf(currentExerciseType) + 1) % types.length];
          if (window.changeExType) window.changeExType(next);
          showToast(labels[next], 1800);
          focusAnswerInput();
        } else if (e.ctrlKey && e.code === "Backspace") {
          e.preventDefault();
          const q =
            currentQuestionsList[currentQIndex % currentQuestionsList.length];
          if (q?.isMastered) {
            const sessId = q._sessId || currentSessionId;
            const realId = q._realId || q.id;
            unmarkWordMasteredInSess(realId, sessId);
          } else {
            masterCurrentWord();
          }
        } else if (e.ctrlKey && e.code === "Space") {
          e.preventDefault();
          moveNext(
            document.getElementById("dynamicAnswerInput"),
            false,
            true,
          );
        } else if (e.ctrlKey && e.altKey) {
          e.preventDefault();
          // Replay audio (listen mode)
          const playBtn = document.getElementById("listenPlayBtn");
          if (playBtn) {
            unlockTTS?.();
            const q = currentQuestionsList?.[currentQIndex % currentQuestionsList.length];
            if (q) speakForMode(q);
          }
        }
      });
    }

    async function generateAndCopyStudyPrompt(promptType, level, withAnswer) {
      const statusEl = document.getElementById("exportPromptStatus");
      if (!promptType) promptType = "vocab";
      if (!level)
        level = document.getElementById("promptLevelSelect")?.value || "A2.2";
      if (statusEl) statusEl.textContent = "⏳ Đang tạo prompt...";

      // ── Level meta ──────────────────────────────────────────────
      const levelMeta = {
        "A1.1": {
          cefr: "A1",
          sub: "1",
          label: "A1.1 (Người mới bắt đầu – tuần 1–8)",
          vocab:
            "Từ vựng cực kỳ cơ bản: chào hỏi, màu sắc, số đếm 1–100, đồ vật trong lớp học, gia đình, thức ăn đơn giản.",
          grammar:
            "Präsens động từ thường (ich/du/er/wir/ihr/sie), mạo từ xác định der/die/das Nominativ, câu khẳng định đơn giản, câu hỏi Ja/Nein và W-Frage cơ bản (wie, was, wer, wo).",
          text: "Câu cực ngắn (3–6 từ), từ đơn lẻ, không Nebensatz, không thì phức tạp.",
          difficulty:
            "Nhận biết và ghi nhớ thuần túy; KHÔNG yêu cầu sản sinh câu phức.",
        },
        "A1.2": {
          cefr: "A1",
          sub: "2",
          label: "A1.2 (Người mới bắt đầu – tuần 9–16)",
          vocab:
            "Mở rộng A1.1: số thứ tự, ngày tháng, thời tiết, màu sắc, quần áo, hoạt động hàng ngày.",
          grammar:
            "Präsens động từ bất quy tắc thường gặp (fahren, lesen, sprechen, essen), mạo từ bất định ein/eine Nominativ và Akkusativ cơ bản, câu phủ định với nicht/kein.",
          text: "Câu ngắn 4–8 từ, đoạn văn tối đa 3–4 câu liên kết.",
          difficulty:
            "Nhận biết + điền từ đơn; bài viết chỉ yêu cầu 1–2 câu.",
        },
        "A2.1": {
          cefr: "A2",
          sub: "1",
          label: "A2.2 (Sơ cấp – giai đoạn 1)",
          vocab:
            "Mua sắm, phương tiện giao thông, hướng dẫn đường, sở thích, thói quen hàng ngày, cơ thể và sức khỏe.",
          grammar:
            "Akkusativ và Dativ với mạo từ xác định/bất định, Präteritum sein/haben, Perfekt động từ đều và bất quy tắc thường gặp, giới từ Wechselpräpositionen cơ bản (in/an/auf + Akk/Dat).",
          text: "Câu 6–10 từ, đoạn văn 4–6 câu, có thể có 1–2 Nebensatz đơn giản (weil, dass).",
          difficulty:
            "Điền từ trong ngữ cảnh câu; bài viết 30–40 từ về chủ đề quen thuộc.",
        },
        "A2.2": {
          cefr: "A2",
          sub: "2",
          label: "A2.2 (Sơ cấp – giai đoạn 2)",
          vocab:
            "Công việc và nghề nghiệp, du lịch và khách sạn, thư từ đơn giản, kế hoạch tương lai, mô tả người và địa điểm.",
          grammar:
            "Biến cách tính từ sau mạo từ xác định (schwache Deklination), Futur I với werden, Modalverben (müssen/können/wollen/dürfen/sollen/mögen) + Infinitiv, câu đảo ngữ (Inversion).",
          text: "Câu 7–12 từ, đoạn văn 60–80 từ, Nebensatz với weil/dass/wenn.",
          difficulty:
            "Chọn đáp án trong ngữ cảnh; bài viết 50–60 từ theo tình huống.",
        },
        "B1.1": {
          cefr: "B1",
          sub: "1",
          label: "B1.1 (Trung cấp – giai đoạn 1)",
          vocab:
            "Môi trường và khí hậu, truyền thông và công nghệ, giáo dục, quan hệ xã hội, cảm xúc và ý kiến cá nhân.",
          grammar:
            "Biến cách tính từ đầy đủ (stark/schwach/gemischt), Relativsatz Nominativ/Akkusativ, Konjunktiv II cơ bản (wäre/hätte/würde+Inf), Passiv Präsens (werden + Partizip II).",
          text: "Câu phức 10–15 từ, đoạn văn 80–120 từ, Relativsatz, câu điều kiện.",
          difficulty:
            "Biến hình từ, phát hiện lỗi sai; bài viết 70–90 từ trình bày quan điểm.",
        },
        "B1.2": {
          cefr: "B1",
          sub: "2",
          label: "B1.2 (Trung cấp – giai đoạn 2)",
          vocab:
            "Kinh tế và tiêu dùng, y tế và hệ thống xã hội, văn hóa và nghệ thuật, thể thao, lập luận và phản biện.",
          grammar:
            "Relativsatz Dativ/Genitiv, Passiv Präteritum, Infinitivkonstruktionen (um…zu / ohne…zu / statt…zu), Konjunktionen phức (obwohl, trotzdem, jedoch, deshalb, daher).",
          text: "Đoạn văn 100–150 từ đa thì, câu phức lồng nhau, văn phong bán trang trọng.",
          difficulty:
            "Chuyển đổi cấu trúc câu, lập luận ngắn; bài viết 90–110 từ có lập luận.",
        },
        "B2.1": {
          cefr: "B2",
          sub: "1",
          label: "B2.1 (Trung-cao cấp – giai đoạn 1)",
          vocab:
            "Chính trị và xã hội, toàn cầu hóa, khoa học và nghiên cứu, triết học và đạo đức cơ bản, văn phong báo chí.",
          grammar:
            "Konjunktiv I (gián tiếp hóa lời nói), Passiv với Modalverben, Partizipialkonstruktion (thay Relativsatz), Genitiv phức tạp, Nominalstil (danh từ hóa).",
          text: "Đoạn văn 150–200 từ văn phong trung tính–trang trọng, nhiều cấu trúc rút gọn.",
          difficulty:
            "Paraphrase câu, chuyển văn phong; bài viết 120–150 từ có luận điểm rõ ràng.",
        },
        "B2.2": {
          cefr: "B2",
          sub: "2",
          label: "B2.2 (Trung-cao cấp – giai đoạn 2)",
          vocab:
            "Kinh tế vĩ mô, luật pháp và quyền công dân, ngôn ngữ học cơ bản, văn học và phê bình, từ vựng học thuật (Wissenschaftssprache).",
          grammar:
            "Toàn bộ Konjunktiv I & II, Partizipialkonstruktion nâng cao, Funktionsverbgefüge (ví dụ: zur Verfügung stellen), cấu trúc đảo ngữ nhấn mạnh (Hervorhebung), văn phong học thuật chính thức.",
          text: "Đoạn văn 200–250 từ văn phong học thuật/báo chí, câu dài đa tầng.",
          difficulty:
            "Phân tích cấu trúc câu, viết luận ngắn; bài viết 150–180 từ lập luận có dẫn chứng.",
        },
      };
      const lm = levelMeta[level] || levelMeta["A2.2"];
      const isCustomLevel = level === "custom";
      const levelBlock = isCustomLevel ? `
## TRÌNH ĐỘ: Tùy chỉnh theo tài liệu đính kèm

**BƯỚC BẮT BUỘC — Phân tích toàn bộ tài liệu trước khi tạo bất kỳ bài tập nào:**

Đọc kỹ toàn bộ file đính kèm (PDF / hình ảnh / text) từ đầu đến cuối. Xác định TẤT CẢ các điểm ngữ pháp xuất hiện xuyên suốt bài — bao gồm:
- Các cấu trúc ngữ pháp được dạy chính thức (bảng, quy tắc, ví dụ mẫu trong phần Grammatik)
- Các cấu trúc ngữ pháp xuất hiện trong hội thoại, bài đọc, bài tập (dù không được giải thích trực tiếp)
- Từ vựng chủ đề và mẫu câu giao tiếp (Kommunikation) nếu có

In bảng phân tích đầy đủ trước khi tạo bài tập:
\`\`\`
📘 Tài liệu: [tên bài / chủ đề]
🎯 Trình độ ước tính: [A1/A2/B1/B2]

📌 CÁC ĐIỂM NGỮ PHÁP XUYÊN SUỐT BÀI:

① [Tên điểm ngữ pháp — tiếng Đức]
   🇻🇳 Giải thích: [giải thích cấu trúc, ý nghĩa và cách dùng bằng tiếng Việt, rõ ràng, dễ hiểu]
   📐 Cấu trúc: [công thức cấu trúc, ví dụ: Subjekt + Verb + Objekt (Dativ)]

② [Tên điểm ngữ pháp — tiếng Đức]
   🇻🇳 Giải thích: [...]
   📐 Cấu trúc: [...]

... (liệt kê hết tất cả điểm ngữ pháp, không giới hạn số lượng, mỗi điểm đầy đủ 2 mục trên)

📚 Từ vựng / mẫu câu chủ đề chính: [liệt kê ngắn]
\`\`\`

> ⚠️ Sau khi in bảng phân tích, mới bắt đầu tạo bài tập. Toàn bộ bài tập phải bao phủ TẤT CẢ các điểm ngữ pháp đã liệt kê — phân bổ đều, không bỏ sót điểm nào. Dùng từ vựng và ngữ cảnh lấy trực tiếp từ tài liệu.
` : `
## TRÌNH ĐỘ: ${level} — ${lm.label}

**Phạm vi từ vựng phù hợp trình độ:** ${lm.vocab}
**Cấu trúc ngữ pháp được dùng:** ${lm.grammar}
**Độ dài và phong cách câu:** ${lm.text}
**Yêu cầu về độ khó bài tập:** ${lm.difficulty}

> ⚠️ Tất cả câu ngữ liệu, đoạn văn, và câu hỏi phải HOÀN TOÀN phù hợp với trình độ ${level}. Không dùng cấu trúc ngữ pháp vượt quá mức trên. Độ khó tăng dần trong từng bài nhưng không vượt ngưỡng trình độ.
`;
      // ────────────────────────────────────────────────────────────

      try {
        let allW = await buildFullListAll();
        // Nếu đang ở chế độ "Chọn từ luyện tập" (custom selection), chỉ xuất
        // đúng những từ đã được chọn/tick trong danh sách đó, không lấy hết toàn bộ phiên.
        if (isCustomMode && window.selectedWordIds?.length) {
          allW = allW.filter((w) => window.selectedWordIds.includes(w.id));
        }

        let prompt = "";

        if (promptType === "image") {
          prompt = `Bạn là trợ lý học tiếng Đức. Tôi sẽ gửi cho bạn một hoặc nhiều hình ảnh hoặc file PDF.

## NHIỆM VỤ
Đọc tài liệu theo đúng thứ tự xuất hiện (trái → phải, trên → dưới; PDF: từng trang theo thứ tự trang) và trích xuất các TỪ CHÍNH tiếng Đức.

## TỪ CHÍNH LÀ
- Danh từ, động từ, tính từ, trạng từ có nghĩa thực
- Giới từ có cách bắt buộc (mit, für, auf, an…) nếu được dạy rõ trong tài liệu

## KHÔNG LẤY
- Mạo từ đứng độc lập (der, die, das khi không kèm danh từ)
- Liên từ đơn giản (und, oder, aber, weil…) trừ khi là trọng tâm bài học
- Số đếm, ký hiệu, tên riêng, tên địa danh thông thường
- Từ lặp lại (chỉ lấy lần đầu xuất hiện)
- Từ trong câu ví dụ minh họa nếu đã có trong danh sách từ chính của bài

## BƯỚC XỬ LÝ BẮT BUỘC (thực hiện nội tâm trước khi xuất)
1. Đọc toàn bộ tài liệu theo thứ tự trang / vùng
2. Xác định đâu là từ chính (tiêu đề bài, bảng từ, in đậm, được giải nghĩa riêng)
3. Lọc bỏ các từ thuộc danh sách KHÔNG LẤY
4. Giữ đúng thứ tự xuất hiện, loại trùng
5. Chỉ sau khi hoàn thành 4 bước trên mới xuất kết quả

## ĐỊNH DẠNG ĐẦU RA — BẮT BUỘC TUYỆT ĐỐI

⚠️ Chỉ xuất các dòng dữ liệu TSV. KHÔNG viết thêm bất cứ thứ gì: không lời mở đầu, không tiêu đề cột, không giải thích, không đánh số, không markdown, không dấu backtick.

Mỗi dòng gồm đúng 4 cột, phân cách bằng TAB (\t), theo thứ tự bất biến:
[Từ tiếng Đức][TAB][Loại từ][TAB][Nghĩa tiếng Việt][TAB][Câu ví dụ (Bản dịch.)]

Ví dụ — xuất y hệt dạng này, không thêm không bớt:
der Apfel / die Äpfel	n	quả táo	Ich esse jeden Tag einen Apfel. (Tôi ăn một quả táo mỗi ngày.)
laufen (du läufst, er läuft, lief, ist gelaufen)	v	chạy	Er läuft jeden Morgen im Park. (Anh ấy chạy trong công viên mỗi buổi sáng.)
helfen (du hilfst, er hilft, half, hat geholfen) [+ D]	v	giúp đỡ	Ich helfe meiner Mutter. (Tôi giúp mẹ tôi.)
an·sehen (du siehst an, er sieht an, sah an, hat angesehen) [+ Akk]	v	nhìn, xem	Er sieht sie an. (Anh ấy nhìn cô ấy.)
kaufen (kaufte, hat gekauft)	v	mua	Sie kauft ein Buch. (Cô ấy mua một cuốn sách.)
machen (machte, hat gemacht)	v	làm	Er macht seine Hausaufgaben. (Anh ấy làm bài tập về nhà.)
auf·räumen (räumte auf, hat aufgeräumt)	v	dọn dẹp	Er räumt sein Zimmer auf. (Anh ấy dọn dẹp phòng.)
kaputt	adj	hỏng, bị vỡ	Das Gerät ist kaputt. (Thiết bị bị hỏng.)

## QUY TẮC CHI TIẾT TỪNG LOẠI TỪ

**Danh từ (loại từ: n):**
- Danh từ thông thường: \`der/die/das [số ít] / die [số nhiều]\`
  - Ví dụ: \`der Tisch / die Tische\`, \`die Frau / die Frauen\`, \`das Kind / die Kinder\`
- Danh từ chỉ số ít (không có số nhiều): loại từ ghi \`n (Sg.)\`
  - Ví dụ: \`der Hunger\` → loại từ: \`n (Sg.)\`
- Danh từ chỉ số nhiều (không có số ít): loại từ ghi \`n (Pl.)\`, ghi từ dạng số nhiều
  - Ví dụ: \`die Leute\` → loại từ: \`n (Pl.)\`

**Động từ (loại từ: v) — Quy tắc ghi dạng chia:**
- **Động từ đều (KHÔNG biến âm du/er):** chỉ ghi \`[nguyên mẫu] (Präteritum, haben/sein + Partizip II)\` — KHÔNG cần ghi du/er vì không đặc biệt
  - Ví dụ: \`kaufen (kaufte, hat gekauft)\`
  - Ví dụ: \`machen (machte, hat gemacht)\`
  - Ví dụ: \`lernen (lernte, hat gelernt)\`
- **Động từ bất quy tắc (CÓ biến âm du/er):** ghi \`[nguyên mẫu] (du [chia du], er [chia er], Präteritum, haben/sein + Partizip II)\` — ghi du/er vì có biến âm đặc biệt
  - Ví dụ: \`fahren (du fährst, er fährt, fuhr, ist gefahren)\`
  - Ví dụ: \`lesen (du liest, er liest, las, hat gelesen)\`
  - Ví dụ: \`schlafen (du schläfst, er schläft, schlief, hat geschlafen)\`
- **Động từ tách (trennbar):** dùng dấu \`·\` để đánh dấu tiền tố tách
  - Nếu động từ gốc KHÔNG biến âm: \`[tiền·tố·nguyên·mẫu] (Präteritum, haben/sein + Partizip II)\`
    - Ví dụ: \`auf·räumen (räumte auf, hat aufgeräumt)\`
    - Ví dụ: \`auf·machen (machte auf, hat aufgemacht)\`
  - Nếu động từ gốc CÓ biến âm du/er: ghi thêm du/er
    - Ví dụ: \`an·rufen (du rufst an, er ruft an, rief an, hat angerufen)\`
    - Ví dụ: \`mit·kommen (du kommst mit, er kommt mit, kam mit, ist mitgekommen)\`
- **Động từ sein/haben/werden và Modalverben:** ghi đủ du/er + Präteritum + Perfekt
  - Ví dụ: \`sein (du bist, er ist, war, ist gewesen)\`
  - Ví dụ: \`haben (du hast, er hat, hatte, hat gehabt)\`
  - Ví dụ: \`können (du kannst, er kann, konnte, hat gekonnt)\`
- **Giới từ đi kèm động từ (nếu có):** ghi sau Partizip II — \`[+ D]\`, \`[+ Akk]\`, \`[+ auf + Akk]\`, \`[+ für + Akk]\`…
  - Ví dụ: \`helfen (du hilfst, er hilft, half, hat geholfen) [+ D]\`
  - Ví dụ: \`warten (wartete, hat gewartet) [+ auf + Akk]\`
  - Ví dụ: \`sich freuen (freute sich, hat sich gefreut) [+ über + Akk / + auf + Akk]\`
- ⚠️ TUYỆT ĐỐI KHÔNG ghi du/er cho động từ đều — chỉ ghi khi có biến âm thực sự

**Tính từ (loại từ: adj):** viết từ đơn giản

**Trạng từ (loại từ: adv):** viết từ đơn giản

**Giới từ (loại từ: prep):** ghi kèm cách — ví dụ: \`mit [+ D]\`, \`für [+ Akk]\`

**Loại khác (loại từ: other):** viết từ đơn giản

## QUY TẮC CHUNG
- Câu ví dụ: ngắn, tự nhiên, đúng ngữ pháp, thể hiện rõ cách dùng của từ — theo sau là bản dịch tiếng Việt trong dấu ngoặc đơn, ví dụ: \`Er schläft. (Anh ấy đang ngủ.)\`
- Nếu tài liệu có sẵn câu ví dụ cho từ đó → ưu tiên dùng câu đó. Nếu KHÔNG có → TỰ TẠO câu ví dụ ngắn, tự nhiên, đúng ngữ pháp, phù hợp nghĩa của từ
- KHÔNG để trống cột câu ví dụ — bắt buộc có ví dụ cho mọi từ
- TUYỆT ĐỐI không thêm văn bản ngoài dữ liệu, không thêm cột, không dùng markdown, không dùng JSON
- Nếu không chắc về một từ → vẫn đưa vào, đừng tự loại bỏ`;
        } else {
          if (!allW.length) {
            if (statusEl) statusEl.textContent = "⚠️ Không có từ vựng!";
            return;
          }

          // Shuffle allW (Fisher-Yates) so vocab appears randomly distributed across exercises
          const shuffledW = [...allW];
          for (let si = shuffledW.length - 1; si > 0; si--) {
            const sj = Math.floor(Math.random() * (si + 1));
            [shuffledW[si], shuffledW[sj]] = [shuffledW[sj], shuffledW[si]];
          }

          const vocabLines = shuffledW
            .map((w, i) => {
              const mastered = w.isMastered ? " [đã thuộc]" : "";
              const type = w.wordType ? ` (${w.wordType})` : "";
              return `${i + 1}. ${w.fullDisplayGerman}${type} — ${w.meaning}${mastered}`;
            })
            .join("\n");

          const totalCount = allW.length;
          const masteredCount = allW.filter((w) => w.isMastered).length;
          const unlearnedCount = totalCount - masteredCount;

          if (promptType === "vocab") {
            prompt = `BẠN LÀ CHUYÊN GIA TẠO ĐỀ THI TIẾNG ĐỨC. Tạo bộ bài tập từ vựng chuẩn Goethe-Zertifikat/telc, trình độ ${level}.

${levelBlock}

QUY TẮC NGỮ PHÁP BẮT BUỘC (áp dụng cho mọi câu):
① Động từ tách (trennbar): câu chính PHẢI có 2 chỗ trống — thân ở V2, tiền tố cuối câu.
   ĐÚNG: "Ich ________ das Fenster ________, weil es heiß ist." (aufmachen → mache / auf)
   SAI: "Ich ________ das Fenster auf." (đã lộ tiền tố)
   Wortkasten ghi dạng chia + tiền tố: "mache ... auf" (KHÔNG ghi nguyên mẫu)
② Động từ phản thân (reflexiv): câu phải có "sich" đúng ngôi (mich/dich/sich/uns/euch).
③ Động từ đòi Dativ (helfen, danken, gefallen, gehören, folgen, antworten...): tân ngữ PHẢI ở Dativ.
④ Động từ bất quy tắc biến âm (fahren→fährt, lesen→liest...): dùng đúng dạng biến âm.
⑤ Danh từ: dùng đúng mạo từ theo giống và Kasus (Nom/Akk/Dat/Gen).
⑥ Tính từ: xác định mạo từ đứng trước (bestimmt/unbestimmt/kein) + Kasus → đuôi tính từ.

NGUYÊN TẮC — VI PHẠM = BÀI TẬP BỊ LOẠI:

❶ PHÂN BỔ TỪ VỰNG
• Danh sách từ ĐÃ XÁO TRỘN NGẪU NHIÊN — phân bổ đều vào TẤT CẢ 8 bài
• Mỗi từ xuất hiện ≥1 lần; từ chưa thuộc xuất hiện 2-3 lần ở bài khác nhau
• KHÔNG tập trung các từ đầu danh sách vào bài 1-2

❷ TÍNH ĐÚNG ĐẮN NGỮ PHÁP — từng câu phải đạt đủ:
  a) ĐỦ NGHĨA: chủ ngữ + vị ngữ hoàn chỉnh
  b) ĐÚNG CHIA: ngôi + thì + trợ động từ (haben/sein)
  c) ĐÚNG CÁCH: mạo từ/tính từ/đại từ đúng Kasus
  d) ĐÚNG VỊ TRÍ: V2 câu chính; cuối Nebensatz; tiền tố tách đúng chỗ
  e) ĐÚNG TỰ NHIÊN: người bản ngữ nói được

❸ ĐÁP ÁN NHIỄU HỢP LỆ
• Từ tiếng Đức thực, đúng chính tả, tra được từ điển; gần đúng về nghĩa/hình thức, dễ nhầm
• Động từ: nhiễu = sai ngôi/thì của CÙNG động từ; danh từ: cùng trường nghĩa, khác giống/cách
• KHÔNG dùng từ vô nghĩa hoặc không tồn tại

❹ PHÂN BỐ A/B/C/D
• Không quá 2 câu liên tiếp cùng đáp án; không quá 35% cùng 1 chữ cái — đếm và hoán đổi nếu lệch

❺ CHECKLIST CUỐI — ĐỌC LẠI TỪNG CÂU TRƯỚC KHI XUẤT:
  ✓ Trennbar → đủ 2 chỗ trống? Wortkasten ghi "mache ... auf"?
  ✓ Reflexiv → "sich" đúng ngôi?
  ✓ Câu hoàn chỉnh, đúng ngữ pháp, không lộ đáp án trong phần còn lại?
  ✓ Không có chú thích gợi ý đáp án "(từ này – logisch)"?
  ✓ Mỗi câu chỉ có 1 chủ ngữ rõ ràng, 1-2 chỗ trống cho 1 từ vựng duy nhất?
  ✓ Đáp án BẮT BUỘC có trong danh sách từ vựng đã cho?
  ✓ Số chỗ trống = số đáp án trong Wortkasten (không tính nhiễu)?
  ✓ Nhiễu là từ thực, có nghĩa? Phân bổ A/B/C/D cân bằng?
  → Câu nào chưa đạt: XÓA và viết lại, KHÔNG sửa vá víu.

QUY TẮC FORMAT:
• Chỗ trống: ngắn (3-4 chữ): ____  trung (5-8): ________  dài (9+): _____________
• Trắc nghiệm: A/B/C/D NGANG 1 dòng, cách nhau 8 khoảng trắng
• Dịch Việt→Đức: câu Việt + dòng kẻ ___________________________________________________
• In ấn: KHÔNG dòng trống giữa câu trong cùng bài; 1 dòng trống giữa các bài; KHÔNG lời mở đầu/kết
• Wortkasten: liệt kê ngang, cách nhau dấu ·

CẤU TRÚC BÀI TẬP (${totalCount} từ: ${unlearnedCount} chưa thuộc, ${masteredCount} đã thuộc)

Bài 1 — Multiple Choice Lückentext (20 câu)
• Mỗi câu: ngữ cảnh hoàn chỉnh + 1 chỗ trống + 4 đáp án A/B/C/D viết NGANG 1 dòng
• KHÔNG dùng Wortkasten — thay hoàn toàn bằng A/B/C/D
• Nhiễu DANH TỪ/TÍNH TỪ: 4 đáp án phải là 4 TỪ KHÁC NHAU hoàn toàn, cùng trường nghĩa hoặc cùng loại từ — KHÔNG ĐƯỢC dùng các dạng biến thể (số ít/số nhiều/giống) của cùng 1 từ làm nhiễu (VD sai: Astronaut/Astronautin/Astronauten/Astronautinnen; VD đúng: Astronaut/Pilot/Ingenieur/Mechaniker)
• Nhiễu ĐỘNG TỪ TÁCH: các đáp án có thể là dạng chia đúng/sai ngôi hoặc tiền tố sai của cùng động từ (VD: A) macht auf   B) mache auf   C) aufmacht   D) macht an)
• Chủ đề đa dạng — không dùng cùng 1 chủ đề quá 5 câu
• ≥4 câu dùng danh từ ở dạng số nhiều (die + danh từ số nhiều, đúng Kasus)
• Phân bổ A/B/C/D: không quá 2 câu liên tiếp cùng đáp án; không quá 30% cùng 1 chữ cái trong 20 câu; đếm và hoán đổi nếu lệch

Bài 2 — Welches Wort passt nicht? (8 nhóm)
• Mỗi nhóm: 4-5 từ cùng trường nghĩa + 1 từ lạc tinh tế
• ≥3 nhóm liệt kê từ ở dạng số nhiều (VD: die Tische · die Stühle · die Lampen · die Fenster · das Buch → ___)
• Format: a) từ1 · từ2 · từ3 · từ4 · từ5 → ___

Bài 3 — Dịch Việt → Đức (10 câu)
• Câu Việt hoàn chỉnh + ... → dòng kẻ dài
• ≥6/10 câu chứa động từ từ danh sách, chia đúng ngôi
• ≥3 câu có danh từ số nhiều trong câu đích (VD: "Tôi thấy những chiếc ghế" → Ich sehe die Stühle.)
• Câu có động từ tách: đáp án mẫu phải đặt tiền tố đúng cuối câu

Bài 4 — Artikel & Pluralformen (12 câu trắc nghiệm)
• 6 câu Artikel số ít: phân bổ ≥2 Nom · ≥2 Akk · ≥1 Dat · ≥1 Nullartikel; 4 đáp án: 1 đúng · 1 sai cách · 1 sai giống · 1 sai loại mạo từ
• 6 câu Pluralformen: mỗi câu cho danh từ số ít → chọn dạng số nhiều đúng (4 đáp án A/B/C/D); bắt buộc đủ 5 kiểu biến đổi (-e · -er · -en · -s · không đổi/Umlaut) · ≥2 Umlaut · ≥1 số nhiều đặc biệt (VD: Museum→Museen)

Bài 5 — Pronomen (8 câu trắc nghiệm)
• 2 Personalpronomen Akk · 2 Personalpronomen Dat · 2 Possessivpronomen · 2 giới từ cố định
• ≥2 câu danh từ ở số nhiều làm tân ngữ (VD: Ich sehe ___ Kinder. → sie)
• 4 đáp án: 1 đúng · 1 sai cách · 1 sai giống · 1 sai hoàn toàn

Bài 6 — Lesetext mit Lücken (đoạn 180 từ, 12 chỗ trống)
• Wortkasten: 15 từ dạng nguyên mẫu/số ít (12 đáp án + 3 nhiễu gần nghĩa thực)
• Phân bổ: ≥6 động từ cần chia đúng · 3 danh từ (trong đó ≥2 ở dạng số nhiều trong đoạn) · 3 từ loại khác
• Động từ tách trong đoạn: chỗ trống ở V2 VÀ tiền tố ở cuối mệnh đề

Bài 7 — Leseverstehen (đoạn 220 từ, 8 câu hỏi trắc nghiệm A/B/C/D)
• Đoạn văn phải có ≥3 câu dùng danh từ số nhiều tự nhiên
• Phân bổ: 3 câu thông tin trực tiếp · 3 câu suy luận · 2 câu từ vựng trong ngữ cảnh
• Mỗi câu: câu hỏi + 4 đáp án A/B/C/D, mỗi đáp án 1 dòng riêng
• Nhiễu: 1 đáp án đúng · 1 gần đúng nhưng sai chi tiết · 1 không có trong đoạn · 1 trái nghĩa/sai hoàn toàn
• Phân bổ đáp án đúng: không quá 2 câu liên tiếp cùng chữ cái; không quá 30% cùng 1 chữ cái trong 8 câu

Bài 8 — Brief schreiben (60–90 từ)
• Ngữ cảnh cụ thể: người viết + người nhận + mục đích + 3 điểm nội dung bắt buộc
• Yêu cầu dùng ≥3 từ từ danh sách; ≥2 danh từ số nhiều trong bài viết; để 10 dòng trống

DANH SÁCH TỪ (ưu tiên từ chưa thuộc — thứ tự đã được xáo trộn ngẫu nhiên)

${vocabLines}

${withAnswer ? "SAU KHI IN ĐỦ ĐỀ BÀI, in ĐÁP ÁN đầy đủ theo từng bài ở cuối." : "⚠️ KHÔNG in đáp án — chỉ in đề bài."}`;
          } else if (promptType === "grammar") {
            prompt = `BẠN LÀ CHUYÊN GIA TẠO ĐỀ THI TIẾNG ĐỨC. Tạo bộ bài tập ngữ pháp NÂNG CAO chuẩn Goethe-Zertifikat/telc, trình độ ${level}.

${levelBlock}

QUY TẮC NGỮ PHÁP BẮT BUỘC (áp dụng cho mọi câu):
① Động từ tách (trennbar) — câu chính: 2 chỗ trống (thân ở V2 + tiền tố cuối).
   "Er ________ das Licht ________." (anmachen → macht / an) — Wortkasten: "macht ... an"
   Nebensatz: tiền tố gắn vào gốc → 1 chỗ trống liền khối: "..., weil er das Licht ________." (anmacht)
   Perfekt: 1 chỗ trống liền khối: "Er hat das Licht ________." (angemacht) — Wortkasten: "angemacht"
② Động từ phản thân (reflexiv): "sich" đúng ngôi (mich/dich/sich/uns/euch) — bắt buộc trong câu.
③ Động từ đòi Dativ (helfen, danken, gefallen, gehören, folgen, antworten, gratulieren...): tân ngữ PHẢI ở Dativ.
④ Động từ bất quy tắc biến âm: du/er: fahren→fährt, lesen→liest, essen→isst; Perfekt với sein: gehen, fahren, kommen...
⑤ Modalverben: Infinitiv của động từ chính đứng CUỐI câu; trennbar + modal → Infinitiv KHÔNG tách.

NGUYÊN TẮC — VI PHẠM = BÀI TẬP BỊ LOẠI:

❶ PHÂN BỔ TỪ VỰNG
• Phân bổ đều vào TẤT CẢ 11 bài; mỗi từ ≥1 lần; động từ chưa thuộc ≥2 lần ở ngôi/thì khác nhau
• KHÔNG tập trung các từ đầu danh sách vào bài đầu

❷ TÍNH ĐÚNG ĐẮN NGỮ PHÁP — từng câu phải đạt đủ:
  a) ĐỦ NGHĨA: chủ ngữ + vị ngữ hoàn chỉnh
  b) ĐÚNG CHIA: ngôi + thì + trợ động từ (haben/sein)
  c) ĐÚNG CÁCH: Kasus đúng cho tất cả thành phần câu
  d) ĐÚNG VỊ TRÍ: V2 câu chính; cuối Nebensatz; tiền tố tách đúng; Partizip II/Infinitiv đúng chỗ
  e) ĐÚNG TỰ NHIÊN: không phải câu dịch máy móc

❸ ĐÁP ÁN NHIỄU HỢP LỆ
• Từ tiếng Đức thực, đúng chính tả; chia động từ: nhiễu = sai ngôi/thì của CÙNG động từ; mạo từ: sai Kasus/Genus nhưng hình thức tồn tại
• KHÔNG dùng dạng chia không tồn tại hoặc từ vô nghĩa

❹ PHÂN BỐ A/B/C/D: không quá 2 câu liên tiếp cùng đáp án; không quá 35% cùng chữ cái — đếm và hoán đổi nếu lệch

❺ CHECKLIST CUỐI — ĐỌC LẠI TỪNG CÂU TRƯỚC KHI XUẤT:
  ✓ Trennbar câu chính → đủ 2 chỗ trống? Wortkasten ghi "macht ... an"?
  ✓ Trennbar Nebensatz/Perfekt → 1 chỗ trống liền khối?
  ✓ Reflexiv → "sich" đúng ngôi?
  ✓ Dativ-Verb → tân ngữ đúng Dativ?
  ✓ 1 chủ ngữ rõ ràng, câu hoàn chỉnh, không lộ đáp án trong phần còn lại?
  ✓ Nhiễu là từ thực, có nghĩa? Phân bổ A/B/C/D cân bằng?
  → Câu nào chưa đạt: XÓA và viết lại, KHÔNG sửa vá víu.

QUY TẮC FORMAT:
• Trắc nghiệm: A/B/C/D NGANG 1 dòng, cách nhau 8 khoảng trắng
• In ấn: KHÔNG dòng trống giữa câu trong cùng bài; 1 dòng trống giữa các bài; KHÔNG lời mở đầu/kết
• Wortkasten: liệt kê ngang, cách nhau ·

CẤU TRÚC BÀI TẬP (${totalCount} từ: ${unlearnedCount} chưa thuộc, ${masteredCount} đã thuộc)

Bài 1 — Konjugation (15 câu, 3 bảng)
• Bảng A — Präsens (6 câu): bắt buộc có động từ bất quy tắc biến âm ở ngôi du, er
  - Động từ tách: 2 chỗ trống (thân ở V2 + tiền tố cuối)
  - Format: [ngữ cảnh] → [chủ ngữ] ________ (nguyên mẫu)
• Bảng B — Perfekt & Präteritum (6 câu): xen kẽ haben/sein; ≥2 Partizip II bất thường
  - Động từ tách trong Perfekt: 1 chỗ trống liền khối (aufgemacht)
• Bảng C — Modalverben (3 câu): điền trợ động từ + Infinitiv cuối câu

Bài 2 — Pronomen & Possessivpronomen (14 câu)
• 5 Personalpronomen Akkusativ · 5 Personalpronomen Dativ
• 4 Possessivpronomen Akk/Dat với đuôi biến cách đúng
• Bắt buộc: ≥2 động từ đòi Dativ cố định · ≥2 giới từ chỉ Akk/Dat

Bài 3 — Adjektivdeklination & Komparation (10 câu)
• 7 câu điền đuôi tính từ (đan xen stark/schwach/gemischt, đủ 4 cách)
• 3 câu Komparativ/Superlativ đúng cấu trúc

Bài 4 — Satzstellung (10 câu)
• 5 câu xáo từ → sắp xếp đúng (bắt buộc có câu có động từ tách → tiền tố phải ra cuối)
• 3 câu → viết lại với Nebensatz (weil/dass/wenn/obwohl)
• 2 câu đảo ngữ (Inversion)
• Dưới mỗi câu: ___________________________________________________

Bài 5 — Zeitformen (10 câu)
• 4 câu Präsens → Perfekt (đan xen haben/sein)
• 3 câu Präsens → Präteritum
• 3 câu → Konjunktiv II (wäre/hätte/würde + Infinitiv)
• Dưới mỗi câu: ___________________________________________________

Bài 6 — Fehlerkorrektur (10 câu)
• Mỗi câu ĐÚNG 1 lỗi; phân bổ: 3 lỗi chia động từ · 2 lỗi vị trí tiền tố tách · 2 lỗi Kasus · 2 lỗi đuôi tính từ · 1 lỗi Partizip II
• Gạch chân từ sai; dưới mỗi câu: ___________________________________________________

Bài 7 — Negation, Frage & Relativsatz (9 câu)
• 3 câu → Negation (nicht/kein đúng vị trí)
• 3 câu → W-Frage (hỏi phần in đậm)
• 3 câu → Relativsatz (Nom/Akk/Dat)
• Dưới mỗi câu: ___________________________________________________

Bài 8 — Passiv (8 câu)
• 4 câu chủ động → Passiv Präsens
• 2 câu → Passiv Präteritum
• 2 câu → Passiv + Modalverb
• Dưới mỗi câu: ___________________________________________________

Bài 9 — Lesetext mit Lücken (đoạn 200 từ, 12 chỗ trống)
• Wortkasten: 15 nguyên mẫu (12 đáp án + 3 nhiễu gần nghĩa)
• Phân bổ: 5 động từ (chia đúng ngôi+thì) · 3 mạo từ/đại từ · 2 tính từ · 2 giới từ/liên từ
• Động từ tách trong đoạn: đảm bảo tiền tố ở đúng cuối mệnh đề

Bài 10 — Leseverstehen (đoạn 220 từ, 8 câu phân tích ngữ pháp)
• Đoạn có Konjunktiv II, Relativsatz, Passiv, Infinitivkonstruktion
• 3 câu: gạch chân cấu trúc + giải thích chức năng
• 2 câu: lý do dùng thì
• 2 câu: viết lại câu (Passiv↔Aktiv)
• 1 câu: liệt kê + phân loại tất cả động từ

Bài 11 — Brief schreiben (80–120 từ)
• Người viết + người nhận + quan hệ + mục đích + 4 điểm nội dung bắt buộc
• Yêu cầu: ≥1 Nebensatz · ≥1 động từ tách đúng cú pháp · đúng thể thức thư
• Để 12 dòng trống

DANH SÁCH TỪ (ưu tiên từ chưa thuộc — thứ tự đã được xáo trộn ngẫu nhiên)

${vocabLines}

${withAnswer ? "SAU KHI IN ĐỦ ĐỀ BÀI, in ĐÁP ÁN đầy đủ theo từng bài ở cuối." : "⚠️ KHÔNG in đáp án — chỉ in đề bài."}`;
          } else if (promptType === "exam") {
            prompt = `BẠN LÀ CHUYÊN GIA TẠO ĐỀ THI TIẾNG ĐỨC. Tạo đề thi thử HOÀN CHỈNH chuẩn Goethe-Zertifikat/telc, trình độ ${level}.

TRÌNH ĐỘ: ${level} — ${lm.label}
Ngữ pháp: ${lm.grammar}
Từ vựng: ${lm.vocab} | Độ dài câu: ${lm.text} | Độ khó: ${lm.difficulty}

⚠️ Dùng từ vựng TỰ NHIÊN, ĐÚNG TRÌNH ĐỘ ${level}, không giới hạn danh sách cụ thể.
⚠️ Động từ tách trong Lückentext: câu chính → 2 chỗ trống (thân V2 + tiền tố cuối); Nebensatz → 1 chỗ trống liền khối.
⚠️ Động từ phản thân: câu phải có "sich" đúng ngôi.

QUY TẮC FORMAT:
• Chỗ trống: ngắn (3-4 chữ): ____  dài (8+): __________
• Trắc nghiệm: A/B/C/D NGANG, cách nhau 8 khoảng trắng — VÍ DỤ: A) der        B) die        C) das
• In ấn: KHÔNG dòng trống giữa câu trong cùng phần; 1 dòng trống giữa các phần; KHÔNG lời mở đầu/kết; Wortkasten liệt kê ngang, cách dấu ·

CẤU TRÚC ĐỀ THI (4 kỹ năng: Lesen, Hören, Schreiben, Sprechen)

TEIL 1 — LESEN (Đọc hiểu)

Aufgabe 1 — Anzeigen/Kurztexte (5 câu Richtig/Falsch hoặc Zuordnung)
• 4-5 thông báo ngắn/biển hiệu/tin nhắn thực tế
• Đúng/Sai hoặc nối với tình huống

Aufgabe 2 — Längerer Text (5 câu trắc nghiệm A/B/C)
• Đoạn văn 150-250 từ, độ khó đúng trình độ
• 5 câu hỏi nội dung/chi tiết
• Đáp án A/B/C viết ngang, cách 8 khoảng trắng

Aufgabe 3 — Lückentext (6-8 chỗ trống, trắc nghiệm A/B/C)
• Đoạn văn có chỗ trống
• Chọn từ/cụm đúng từ A/B/C
• Chỗ trống có độ dài phù hợp

TEIL 2 — HÖREN (Nghe hiểu — mô phỏng script)

⚠️ Cung cấp TRANSCRIPT đầy đủ, ghi rõ "TRANSCRIPT:" trước mỗi đoạn

Aufgabe 1 — Kurze Gespräche (5-6 đoạn hội thoại ngắn)
• Mỗi đoạn: 3-5 lượt thoại
• TRANSCRIPT → 1 câu hỏi A/B/C hoặc Richtig/Falsch
• Đáp án viết ngang, cách 8 khoảng trắng

Aufgabe 2 — Längeres Gespräch/Ansage (1 đoạn 150-200 từ)
• TRANSCRIPT đầy đủ
• 4-5 câu hỏi nội dung A/B/C
• Đáp án viết ngang, cách 8 khoảng trắng

TEIL 3 — SCHREIBEN (Viết)

Aufgabe 1 — Formular/Kurznachricht (40-60 từ)
• Tình huống thực tế
• Ghi rõ 3 điểm nội dung bắt buộc
• Để 5-6 dòng trống cho học viên viết

Aufgabe 2 — Längerer Text (70-120 từ tùy trình độ)
• Tình huống cụ thể
• Ghi rõ yêu cầu nội dung và hình thức
• Để 8-10 dòng trống cho học viên viết

⚠️ Cung cấp bài mẫu (Musterlösung) ở phần Đáp án

TEIL 4 — SPRECHEN (Nói — mô phỏng thẻ bài thi)

Aufgabe 1 — Sich vorstellen (Tự giới thiệu)
• Liệt kê 5-6 gợi ý nội dung (Thema-Chips)
• Phù hợp trình độ

Aufgabe 2 — Thema + Fragen (Thảo luận chủ đề)
• 1 chủ đề thực tế
• 6 thẻ từ khóa
• Mỗi thẻ: câu hỏi gợi ý + câu trả lời mẫu ngắn

Aufgabe 3 — Bitte/Reaktion (Yêu cầu và phản hồi)
• 2 tình huống: đưa ra yêu cầu lịch sự + phản hồi
• Kèm câu mẫu

${withAnswer ? "SAU KHI IN ĐỦ ĐỀ BÀI, in ĐÁP ÁN đầy đủ theo từng bài ở cuối." : "⚠️ KHÔNG in đáp án — chỉ in đề bài."}`;
          } else if (promptType === "sprechen-t2") {
            // ── Build thema list from vocab ──────────────────────
            const themaWords = allW.length
              ? allW
                .map(
                  (w) =>
                    w.fullDisplayGerman || w.mainGerman || w.originalGerman,
                )
                .filter(Boolean)
                .slice(0, 30)
              : [];
            const themaList = themaWords.length
              ? themaWords.map((w, i) => `${i + 1}. ${w}`).join("\n")
              : "(Không có từ vựng — hãy tự chọn 6–8 từ khóa phù hợp chủ đề)";

            prompt = `Bạn là giáo viên tiếng Đức luyện thi, mô phỏng đúng format kỳ thi **Start Deutsch 1 / Goethe-Zertifikat A1 — Sprechen Teil 2** (và tương đương các cấp độ khác tùy trình độ đã chọn).
${levelBlock}
## FORMAT THI — SPRECHEN TEIL 2
Mỗi người nhận thẻ có 1 từ khóa lớn + Thema. Người A đọc thẻ → đặt câu hỏi tiếng Đức → Người B trả lời câu hoàn chỉnh. Sau đó đổi vai.

## NHIỆM VỤ

Tạo **2 bộ thẻ Sprechen Teil 2** từ danh sách từ vựng bên dưới, mỗi bộ có Thema khác nhau, mỗi bộ gồm **6 thẻ từ khóa**.

Với mỗi thẻ:
\`\`\`
Thẻ [số]: [TỪ KHÓA IN ĐẬM LỚN]
├─ Câu hỏi gợi ý (in nghiêng): Wie / Was / Wann / Wo / Wohin / Warum…?
└─ Câu trả lời mẫu: Ich… / Das… / Mein/e… (câu hoàn chỉnh, đúng ngữ pháp)
\`\`\`

**Yêu cầu nội dung:**
- Từ khóa lấy từ danh sách (ưu tiên từ chưa thuộc); nếu không đủ thì chọn từ phù hợp cùng trình độ
- Câu hỏi: tiếng Đức thật, đúng ngữ pháp (W-Frage hoặc Ja/Nein-Frage)
- Câu trả lời: đúng ngữ pháp, tự nhiên, ≥4 từ, khớp trình độ ${level}
- Hai bộ thẻ phải có Thema khác nhau (ví dụ: Alltag và Freizeit; Wohnen và Einkaufen…)
- KHÔNG dịch sang tiếng Việt trong phần thẻ; mỗi bộ có thể in ra và dùng được ngay trong lớp

## DANH SÁCH TỪ VỰNG HIỆN TẠI

${themaList}`;
          } else if (promptType === "sprechen-t3") {
            const objectWords = allW.length
              ? allW
                .filter((w) => w.wordType === "n" || !w.wordType)
                .map(
                  (w) =>
                    w.fullDisplayGerman || w.mainGerman || w.originalGerman,
                )
                .filter(Boolean)
                .slice(0, 20)
              : [];
            const objectList = objectWords.length
              ? objectWords.map((w, i) => `${i + 1}. ${w}`).join("\n")
              : "(Không có danh từ — AI tự chọn đồ vật phù hợp trình độ)";

            prompt = `Bạn là giáo viên tiếng Đức luyện thi, mô phỏng đúng format kỳ thi **Start Deutsch 1 / Goethe-Zertifikat A1 — Sprechen Teil 3** (và tương đương các cấp độ khác tùy trình độ đã chọn).
${levelBlock}
## FORMAT THI — SPRECHEN TEIL 3
Mỗi người nhận 2 thẻ hình đồ vật. Người A giơ thẻ → nói lời nhờ/yêu cầu → Người B phản hồi đồng ý hoặc từ chối lịch sự. Sau đó đổi thẻ và đổi vai.

## NHIỆM VỤ

Tạo **6 thẻ hình Sprechen Teil 3** từ danh sách danh từ bên dưới (ưu tiên đồ vật hữu hình, có thể mượn/nhờ được).

Với mỗi thẻ:
\`\`\`
Thẻ [số]: 🖼 [TÊN ĐỒ VẬT tiếng Đức — in đậm]  (Loại: [der/die/das])
│
├─ Lời nhờ A (lịch sự cơ bản):
│   "Kannst du mir bitte ___ geben?" / "Darf ich ___ nehmen?" / "Hast du ___?"
│
├─ Lời nhờ B (lịch sự nâng cao, phù hợp trình độ ${level}):
│   (Konjunktiv II "Könntest du…" hoặc cấu trúc phức hơn nếu B1+)
│
├─ Phản hồi đồng ý: "Ja, natürlich! Hier bitte." / "Gerne!" / "Kein Problem, ich…"
│
└─ Từ chối lịch sự: "Tut mir leid, ich habe kein/e/en ___ dabei." / "Es tut mir leid, aber…"
\`\`\`

**Yêu cầu:**
- Mỗi thẻ: 1 đồ vật cụ thể, hữu hình, có thể nhờ mượn trong ngữ cảnh thực tế
- Akkusativ của mạo từ phải chính xác (den/die/das/einen/eine/ein)
- Lời nhờ B khó hơn A nhưng không vượt ngưỡng trình độ ${level}
- Phản hồi đa dạng (không lặp cùng mẫu câu); toàn bộ bằng tiếng Đức
- Nếu danh sách không đủ đồ vật phù hợp, tự bổ sung đồ vật đúng trình độ ${level}

## DANH SÁCH DANH TỪ HIỆN TẠI

${objectList}`;
          } else if (promptType === "translation") {
            if (!allW.length) {
              if (statusEl) statusEl.textContent = "⚠️ Không có từ vựng!";
              return;
            }
            const shuffledW2 = [...allW];
            for (let si = shuffledW2.length - 1; si > 0; si--) {
              const sj = Math.floor(Math.random() * (si + 1));
              [shuffledW2[si], shuffledW2[sj]] = [shuffledW2[sj], shuffledW2[si]];
            }
            const vocabLinesT = shuffledW2
              .map((w, i) => {
                const type = w.wordType ? ` (${w.wordType})` : "";
                const mastered = w.isMastered ? " [đã thuộc]" : "";
                return `${i + 1}. ${w.fullDisplayGerman}${type} — ${w.meaning}${mastered}`;
              })
              .join("\n");

            prompt = `BẠN LÀ CHUYÊN GIA TẠO ĐỀ DỊCH TIẾNG ĐỨC. Tạo bộ bài tập dịch chuẩn trình độ ${level}.

${levelBlock}

YÊU CẦU CHUNG:
• Tạo đúng 5 bài tập dịch, mỗi bài 10 câu (bài 5 là đoạn văn đặc biệt — xem dưới).
• Mỗi bài dùng từ vựng trong danh sách bên dưới — phân bổ đều, ưu tiên từ chưa thuộc.
• Phân bổ cấu trúc ngữ pháp đều qua 10 câu: ① chia động từ đúng ngôi/thì (Präsens, Perfekt, Präteritum) ② tính từ đúng Kasus + loại mạo từ ③ đại từ sở hữu đúng ngôi + Kasus ④ liên từ kết hợp (und/aber/oder/denn/sondern) + liên từ phụ thuộc (weil/dass/wenn/obwohl…) + um...zu (đúng vị trí động từ trong mệnh đề phụ) ⑤ Perfekt (haben/sein + Partizip II)
• Bài 1–4: mỗi câu là 1 câu tiếng Việt → người học dịch sang tiếng Đức.
• Bài 5: 1 đoạn văn tiếng Việt (~8–10 câu liên kết) → người học dịch toàn bộ đoạn.
• Động từ tách trong bài dịch: đáp án mẫu phải đặt tiền tố đúng cuối mệnh đề.

QUY TẮC TỪNG BÀI:

Bài 1 — Cơ bản: câu đơn giản, Präsens, chia động từ thông thường, tính từ vị ngữ.
  → Mạo từ Nom/Akk; tính từ đứng sau sein/werden.

Bài 2 — Tính từ & Đại từ sở hữu: tính từ trước danh từ (đúng đuôi biến cách), đại từ sở hữu ≥5/10 câu.

Bài 3 — Perfekt: haben/sein + Partizip II đúng; ≥3 động từ dùng "sein" (gehen, kommen, fahren…); ≥4 động từ đều, ≥4 bất quy tắc.

Bài 4 — Liên từ & Mệnh đề phụ: liên từ kết hợp (und/aber/oder/denn/sondern — động từ vị trí 2 ở cả 2 vế) ≥2 câu; liên từ phụ thuộc (weil/dass/wenn/obwohl/damit/bevor/nachdem…, động từ chia đứng cuối mệnh đề phụ) ≥4 câu; cấu trúc um...zu + Infinitiv (đúng cụm động từ ở cuối) ≥2 câu; còn lại là câu ghép có liên từ khác hoặc câu đơn có trạng từ liên kết (deshalb/trotzdem/außerdem).

Bài 5 — Đoạn văn dịch: 1 đoạn tiếng Việt mạch lạc (~8–10 câu), chủ đề cụ thể (kể buổi sáng, mô tả ai đó, kể chuyến đi…). Dùng nhiều từ trong danh sách. Kết hợp Präsens + Perfekt + Präteritum + tính từ + đại từ sở hữu + ≥1 động từ tách (tiền tố đúng cuối câu) trong cùng đoạn. Đặt tiêu đề cho đoạn. In rõ: "📝 Dịch toàn bộ đoạn văn sau sang tiếng Đức:" → để 15 dòng kẻ trống.

QUY TẮC FORMAT:
1. Tiêu đề: BÀI [số] — [tên loại bài]
2. Mỗi câu: đánh số 1–10, câu tiếng Việt + dòng kẻ: ___________________________________________________
3. Không dòng trống giữa câu trong cùng bài; 1 dòng trống giữa các bài.
4. Bài 5: đoạn văn in liền, sau đó để dòng kẻ trống (không gạch chân từng câu).
5. KHÔNG lời mở đầu, lời kết, giải thích thêm.
6. ${withAnswer ? "In đáp án (bản dịch tiếng Đức) ở cuối sau khi in xong toàn bộ 5 bài đề." : "KHÔNG viết đáp án — chỉ in đề."}

DANH SÁCH TỪ VỰNG (ưu tiên từ chưa thuộc — đã xáo trộn ngẫu nhiên)

${vocabLinesT}`;
          }
        }

        // Copy to clipboard
        try {
          await navigator.clipboard.writeText(prompt);
        } catch (e) {
          const ta = document.createElement("textarea");
          ta.value = prompt;
          ta.style.cssText = "position:fixed;opacity:0;top:0;left:0";
          document.body.appendChild(ta);
          ta.focus();
          ta.select();
          document.execCommand("copy");
          document.body.removeChild(ta);
        }

        const labels = {
          vocab: "từ vựng",
          grammar: "ngữ pháp",
          image: "trích ảnh",
          exam: "đề thi thử",
          "sprechen-t2": "Luyện nói Teil 2",
          "sprechen-t3": "Luyện nói Teil 3",
          translation: "bài tập dịch",
        };
        const count = ["image", "sprechen-t2", "sprechen-t3", "translation"].includes(
          promptType,
        )
          ? ""
          : ` (${allW.length} từ)`;
        if (statusEl)
          statusEl.textContent = `✅ Đã copy prompt ${labels[promptType]}${count}!`;
        setTimeout(() => {
          if (statusEl) statusEl.textContent = "";
        }, 4000);
      } catch (err) {
        console.error(err);
        if (statusEl) statusEl.textContent = "❌ Lỗi khi tạo prompt";
      }
    }

    function initEvents() {
      document
        .querySelector("#header > div:first-child")
        ?.addEventListener("click", () => {
          isDarkMode = !isDarkMode;
          localStorage.setItem("darkMode", isDarkMode);
          applyDark();
          updateAllToggles();
          showToast(isDarkMode ? "🌙 Tối" : "☀️ Sáng");
        });
      document
        .getElementById("sidebarToggle")
        .addEventListener("click", () => {
          const sb = document.getElementById("sidebar");
          sb.classList.toggle("collapsed");
          document.getElementById("sidebarToggle").textContent =
            sb.classList.contains("collapsed") ? "›" : "‹";
        });
      document.getElementById("tgAllowSkip").addEventListener("click", () => {
        allowSkip = !allowSkip;
        localStorage.setItem("allowSkip", allowSkip);
        updateAllToggles();
        showToast(allowSkip ? "⏭ Bỏ qua: BẬT" : "🔒 Bỏ qua: TẮT");
      });
      // ── Search history helpers (dùng cho modal tìm kiếm toàn phiên) ──
      const SEARCH_HIST_KEY = "sidebarSearchHistory";
      const SEARCH_HIST_MAX = 12;
      function getSearchHistory() {
        try { return JSON.parse(localStorage.getItem(SEARCH_HIST_KEY) || "[]"); } catch { return []; }
      }
      function pushSearchHistory(q) {
        if (!q || q.trim().length < 2) return;
        let h = getSearchHistory().filter(x => x !== q);
        h.unshift(q);
        if (h.length > SEARCH_HIST_MAX) h = h.slice(0, SEARCH_HIST_MAX);
        localStorage.setItem(SEARCH_HIST_KEY, JSON.stringify(h));
      }

      // ── Sidebar search (chỉ tìm trong phiên hiện tại / gộp) ──
      function updateSearchClear() {
        const inp = document.getElementById("sidebarSearch");
        const btn = document.getElementById("sidebarSearchClear");
        if (!inp || !btn) return;
        btn.style.display = inp.value ? "block" : "none";
        inp.style.paddingRight = inp.value ? "28px" : "";
      }
      document.getElementById("sidebarSearch")?.addEventListener("input", (e) => {
        sidebarFilter = e.target.value;
        sidebarPage = 1;
        updateSearchClear();
        renderSidebar();
      });
      document.getElementById("sidebarSearchClear")?.addEventListener("click", () => {
        const inp = document.getElementById("sidebarSearch");
        if (inp) { inp.value = ""; inp.focus(); }
        sidebarFilter = "";
        sidebarPage = 1;
        updateSearchClear();
        renderSidebar();
      });
      updateSearchClear();

      // ── Global Search Modal (tìm trong tất cả phiên) ──
      async function runGlobalSearch(q) {
        const resultsEl = document.getElementById("globalSearchResults");
        const countEl = document.getElementById("globalSearchCount");
        if (!resultsEl) return;
        if (!q || q.trim().length < 1) {
          resultsEl.innerHTML = "";
          if (countEl) countEl.textContent = "";
          return;
        }
        const sessions = await dbGetAllSessions();
        const needle = normSearch(q.trim());
        let allResults = [];
        for (const sess of sessions) {
          const vocab = await dbGetSessionVocab(sess.id);
          const mIds = await dbGetMastered(sess.id);
          const fIds = await dbGetFlagged(sess.id);
          vocab.forEach(item => {
            if (normSearch(item.originalGerman).includes(needle) || normSearch(item.meaning).includes(needle)) {
              allResults.push({ ...item, _sessId: sess.id, _sessName: sess.name, _isMastered: mIds.has(item.id), _isFlagged: fIds.has(item.id) });
            }
          });
        }
        if (countEl) countEl.textContent = allResults.length ? `${allResults.length} kết quả trong ${sessions.length} phiên` : "";
        if (!allResults.length) {
          resultsEl.innerHTML = `<div style="text-align:center;padding:24px 0;color:var(--tx3);font-size:0.88rem">Không tìm thấy từ nào</div>`;
          return;
        }
        // Group by session
        const bySession = {};
        allResults.forEach(r => {
          if (!bySession[r._sessId]) bySession[r._sessId] = { name: r._sessName, items: [] };
          bySession[r._sessId].items.push(r);
        });
        let html = "";
        for (const [sid, group] of Object.entries(bySession)) {
          html += `<div style="margin-bottom:2px"><div style="font-size:0.72rem;font-weight:700;color:var(--tx3);padding:6px 8px 3px;text-transform:uppercase;letter-spacing:.05em">${escapeHtml(group.name)}</div>`;
          html += group.items.map(v => {
            // Highlight: match against normalized string, then map back to original with dots preserved
            const hlStr = (original) => {
              const escaped = escapeHtml(original);
              const needleEsc = needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
              // Build regex that allows optional · between each char pair
              const needleWithDots = needleEsc.split('').join('·?');
              try {
                return escaped.replace(new RegExp(`(${needleWithDots})`, 'gi'), '<mark style="background:rgba(88,166,255,.28);color:inherit;border-radius:2px">$1</mark>');
              } catch { return escaped; }
            };
            const badge = v._isMastered ? `<span style="font-size:0.62rem;background:rgba(63,185,80,.18);border:1px solid rgba(63,185,80,.4);color:#3fb950;padding:1px 5px;border-radius:8px;flex-shrink:0"><i class="fa-solid fa-star"></i></span>` : v._isFlagged ? `<span style="font-size:0.62rem;background:rgba(240,192,0,.13);border:1px solid rgba(240,192,0,.35);color:#f0c000;padding:1px 5px;border-radius:8px;flex-shrink:0"><i class="fa-solid fa-star"></i></span>` : "";
            return `<div style="display:flex;align-items:center;gap:7px;padding:7px 10px;border-radius:6px;cursor:pointer;transition:background .15s" class="gs-result-item" data-sessid="${sid}" data-id="${v.id}" onmouseover="this.style.background='var(--bg3)'" onmouseout="this.style.background=''">
              <div style="flex:1;min-width:0">
                <div style="font-size:0.84rem;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${hlStr(v.originalGerman)}${v.wordType ? `<span style="font-size:0.62rem;background:var(--bg3);border:1px solid var(--border);color:var(--tx3);padding:1px 5px;border-radius:4px;margin-left:5px">${escapeHtml(v.wordType)}</span>` : ""}</div>
                <div style="font-size:0.75rem;color:var(--tx3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:'DM Mono',monospace">${hlStr(v.meaning)}</div>
              </div>
              ${badge}
            </div>`;
          }).join("");
          html += `</div>`;
        }
        resultsEl.innerHTML = html;
        // Click to go to session
        resultsEl.querySelectorAll(".gs-result-item").forEach(el => {
          el.addEventListener("click", async () => {
            const sessId = el.dataset.sessid;
            if (sessId !== currentSessionId) await switchSession(sessId);
            closeModal("globalSearchModal");
          });
        });
      }

      function openGlobalSearchModal() {
        openModal("globalSearchModal");
        setTimeout(() => document.getElementById("globalSearchInput")?.focus(), 80);
        renderGlobalSearchHistory();
        // Run search with last query if any
        const inp = document.getElementById("globalSearchInput");
        if (inp?.value) runGlobalSearch(inp.value);
      }
      function renderGlobalSearchHistory() {
        const hist = getSearchHistory();
        const wrap = document.getElementById("globalSearchHistory");
        const list = document.getElementById("globalSearchHistoryList");
        if (!wrap || !list) return;
        if (!hist.length) { wrap.style.display = "none"; return; }
        wrap.style.display = "block";
        list.innerHTML = hist.map(h => `<button class="gsh-tag" data-h="${escapeHtml(h)}" style="background:var(--bg3);border:1px solid var(--border);color:var(--tx2);font-size:0.78rem;padding:3px 9px;border-radius:20px;cursor:pointer;font-family:inherit;display:inline-flex;align-items:center;gap:4px;transition:all .15s" onmouseover="this.style.borderColor='#58a6ff';this.style.color='#58a6ff'" onmouseout="this.style.borderColor='';this.style.color='var(--tx2)'">${escapeHtml(h)} <span style="color:var(--tx3);font-size:0.85rem" data-del="${escapeHtml(h)}">×</span></button>`).join("");
        list.querySelectorAll(".gsh-tag").forEach(btn => {
          btn.addEventListener("click", (e) => {
            const delSpan = e.target.closest("[data-del]");
            if (delSpan) {
              const h = delSpan.dataset.del;
              const newHist = getSearchHistory().filter(x => x !== h);
              localStorage.setItem(SEARCH_HIST_KEY, JSON.stringify(newHist));
              renderGlobalSearchHistory();
              return;
            }
            const h = btn.dataset.h;
            const inp = document.getElementById("globalSearchInput");
            if (inp) {
              inp.value = h;
              document.getElementById("globalSearchClearBtn").style.display = "block";
            }
            doGlobalSearch();
          });
        });
      }
      document.getElementById("clearAllSearchHistory")?.addEventListener("click", () => {
        localStorage.removeItem(SEARCH_HIST_KEY);
        renderGlobalSearchHistory();
      });
      function doGlobalSearch() {
        const inp = document.getElementById("globalSearchInput");
        const q = inp ? inp.value : "";
        runGlobalSearch(q);
        if (q.trim().length >= 2) {
          pushSearchHistory(q.trim());
          renderGlobalSearchHistory();
        }
      }
      document.getElementById("globalSearchInput")?.addEventListener("input", (e) => {
        const q = e.target.value;
        const clearBtn = document.getElementById("globalSearchClearBtn");
        if (clearBtn) clearBtn.style.display = q ? "block" : "none";
      });
      document.getElementById("globalSearchInput")?.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          doGlobalSearch();
        }
      });
      document.getElementById("globalSearchBtn")?.addEventListener("click", () => {
        doGlobalSearch();
        document.getElementById("globalSearchInput")?.focus();
      });
      document.getElementById("globalSearchClearBtn")?.addEventListener("click", () => {
        const inp = document.getElementById("globalSearchInput");
        if (inp) { inp.value = ""; inp.focus(); }
        document.getElementById("globalSearchClearBtn").style.display = "none";
        document.getElementById("globalSearchResults").innerHTML = "";
        document.getElementById("globalSearchCount").textContent = "";
      });
      document.getElementById("closeGlobalSearchModal")?.addEventListener("click", () => closeModal("globalSearchModal"));

      // ── ESC để đóng modal đang mở (bất kể modal nào) ──
      document.addEventListener("keydown", (e) => {
        if (e.key !== "Escape") return;
        const openModalEl = document.querySelector(".modal-overlay.open");
        if (openModalEl) {
          e.preventDefault();
          openModalEl.classList.remove("open");
        }
      });

      // ── Click-outside handler chung cho tất cả modal (dùng mousedown để tránh bug drag) ──
      // Ghi nhớ vị trí mousedown để chỉ đóng khi cả mousedown lẫn mouseup đều ở overlay
      let _modalMousedownTarget = null;
      document.addEventListener("mousedown", (e) => {
        _modalMousedownTarget = e.target;
      });
      document.addEventListener("mouseup", (e) => {
        const overlay = e.target.closest(".modal-overlay.open");
        if (
          overlay &&
          overlay === e.target &&                        // mouseup trực tiếp trên overlay
          _modalMousedownTarget === overlay &&           // mousedown cũng trên overlay (không phải drag từ trong ra)
          !window.matchMedia("(pointer: coarse)").matches // chỉ desktop (pointer chuột)
        ) {
          closeModal(overlay.id);
        }
        _modalMousedownTarget = null;
      });

      // headerSearchAllBtn → opens modal
      document.getElementById("headerSearchAllBtn")?.addEventListener("click", openGlobalSearchModal);

      document
        .getElementById("sidebarFilterSelect")
        .addEventListener("change", (e) => {
          sidebarFilterTab = e.target.value;
          sidebarPage = 1;
          renderSidebar();
        });
      document
        .getElementById("addWordSidebarBtn")
        .addEventListener("click", () => {
          if (isMobileView()) closeMobileSidebar();
          openAddModal();
        });
      document
        .getElementById("sidebarTypeFilter")
        ?.addEventListener("change", (e) => {
          sidebarTypeFilter = e.target.value;
          sidebarPage = 1;
          renderSidebar();
        });
      document
        .getElementById("headerSelectBtn")
        ?.addEventListener("click", () => openSelectWordsModal());
      document
        .getElementById("folderMgrBtn")
        ?.addEventListener("click", async () => {
          await renderFolderModal();
          openModal("folderModal");
        });
      ["closeFolderModal", "closeFolderDone"].forEach((id) =>
        document
          .getElementById(id)
          ?.addEventListener("click", () => closeModal("folderModal")),
      );
      ["closeAddSessModal", "cancelAddSessModal"].forEach((id) =>
        document.getElementById(id)?.addEventListener("click", () => closeModal("addSessModal")),
      );
      document.getElementById("addSessModal")?.addEventListener("click", (e) => {
        // handled by unified mousedown/mouseup handler above
      });
      document.getElementById("confirmAddSessModal")?.addEventListener("click", async () => {
        const fid = document.getElementById("confirmAddSessModal").dataset.fid;
        if (!fid) return;
        const checked = document.querySelectorAll("#addSessModalList .add-sess-cb:checked");
        if (!checked.length) return;
        closeModal("addSessModal");
        showLoading("");
        try {
          await Promise.all([...checked].map((cb) => setSessionFolder(cb.dataset.sessid, fid)));
        } finally { hideLoading(); }
        await renderFolderModal();
        await renderSessionDropdowns();
      });
      document
        .getElementById("folderModal")
        ?.addEventListener("click", (e) => {
          // handled by unified mousedown/mouseup handler above
        });
      // Import button in folder modal
      document
        .getElementById("importSessionsBtnModal")
        ?.addEventListener("click", () => {
          openImportSessionsModal("folderModal");
        });
      // Legacy export/import button (if present)
      document
        .getElementById("exportImportSessionsBtn")
        ?.addEventListener("click", () => {
          openImportSessionsModal("folderModal");
        });
      document
        .getElementById("createFolderBtn")
        ?.addEventListener("click", async () => {
          const inp = document.getElementById("newFolderName");
          const name = inp?.value.trim();
          if (!name) return;
          const newF = await createFolder(name);
          if (inp) inp.value = "";
          // Auto-expand newly created folder so user can immediately add sessions
          if (!window._folderCollapsed) window._folderCollapsed = {};
          window._folderCollapsed[newF.id] = false;
          await renderFolderModal();
          await renderSessionDropdowns();
        });
      document
        .getElementById("newFolderName")
        ?.addEventListener("keydown", async (e) => {
          if (e.key === "Enter")
            document.getElementById("createFolderBtn")?.click();
        });
      document
        .getElementById("settingsBtn")
        .addEventListener("click", async () => {
          updateAllToggles();
          document.getElementById("sourceSelectModal").value = currentSource;
          const wlm = document.getElementById("wordLimitSelectModal");
          if (wlm) wlm.value = String(wordLimit);
          const etm = document.getElementById("exerciseTypeSelectModal");
          if (etm) etm.value = currentExerciseType;
          await renderSessionDropdowns();
          const user =
            sessionStorage.getItem("loggedUser") ||
            localStorage.getItem("loggedUser") ||
            "Người dùng";
          const nameEl = document.getElementById("settingsUsername"),
            avatar = document.getElementById("settingsUserAvatar");
          if (nameEl) nameEl.textContent = user;
          if (avatar) avatar.textContent = user.charAt(0).toUpperCase();
          // Render stats
          const allWords = await buildFullListAll();
          const totalMastered = allWords.filter((v) => v.isMastered).length;
          const totalFlagged = allWords.filter((v) => v.isFlagged).length;
          const sessions = await dbGetAllSessions();
          const statsEl = document.getElementById("settingsUserStats");
          if (statsEl) {
            statsEl.innerHTML = `
          <span style="color:var(--tx3)"><i class="fa-solid fa-folder"></i> ${sessions.length} phiên</span>
          <span style="color:var(--tx3)">·</span>
          <span style="color:var(--tx2)"><i class="fa-solid fa-book"></i> ${allWords.length} từ</span>
          <span style="color:var(--tx3)">·</span>
          <span style="color:#3fb950"><i class="fa-solid fa-circle-check"></i> ${totalMastered}</span>
          <span style="color:var(--tx3)">·</span>
          <span style="color:#f0c000"><i class="fa-solid fa-star"></i> ${totalFlagged}</span>
        `;
          }
          openModal("settingsModal");
        });
      document.getElementById("logoutBtn")?.addEventListener("click", async () => {
        if (await customConfirm("Bạn có chắc muốn đăng xuất?")) {
          sessionStorage.clear();
          localStorage.removeItem("loggedIn");
          localStorage.removeItem("loggedUser");
          localStorage.removeItem("loggedUid");
          window.location.replace("login.html");
        }
      });
      document
        .getElementById("optionsSectionToggle")
        ?.addEventListener("click", () => {
          const panel = document.getElementById("optionsPanel"),
            icon = document.getElementById("optionsToggleIcon");
          if (!panel) return;
          const isHidden = panel.style.display === "none";
          panel.style.display = isHidden ? "block" : "none";
          if (icon) icon.textContent = isHidden ? "▼" : "▶";
        });
      document
        .getElementById("breakSectionToggle")
        ?.addEventListener("click", () => {
          const panel = document.getElementById("breakPanel"),
            icon = document.getElementById("breakToggleIcon");
          if (!panel) return;
          const isHidden = panel.style.display === "none";
          panel.style.display = isHidden ? "block" : "none";
          if (icon) icon.textContent = isHidden ? "▼" : "▶";
        });
      document
        .getElementById("aiPromptSectionToggle")
        ?.addEventListener("click", () => {
          const panel = document.getElementById("aiPromptPanel"),
            icon = document.getElementById("aiPromptToggleIcon");
          if (!panel) return;
          const isHidden = panel.style.display === "none";
          panel.style.display = isHidden ? "block" : "none";
          if (icon) icon.textContent = isHidden ? "▼" : "▶";
        });
      document
        .getElementById("newSessionBtnModal")
        .addEventListener("click", createNewSession);
      document;

      // Session switching now handled via folder modal picker
      // exportImportSessionsBtn now lives in folder modal — handled via renderFolderModal
      document
        .getElementById("closeExportSessionsModal")
        .addEventListener("click", () => {
          closeModal("exportSessionsModal");
        });
      document
        .getElementById("cancelExportSessionsModal")
        .addEventListener("click", () => {
          closeModal("exportSessionsModal");
        });
      document
        .getElementById("exportSessionsModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "exportSessionsModal") {
            closeModal("exportSessionsModal");
          }
        });
      document
        .getElementById("confirmExportSessionsBtn")
        .addEventListener("click", async () => {
          const selected = [
            ...document.querySelectorAll(
              "#exportSessionsList .export-sess-cb:checked",
            ),
          ].map((cb) => cb.dataset.id);
          if (!selected.length) {
            showToast("⚠️ Chọn ít nhất 1 phiên!");
            return;
          }
          showLoading("Đang xuất...");
          try {
            const data = await exportSessionsToJson(selected);
            const date = new Date().toISOString().slice(0, 10);
            downloadBlob(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }), `phien-hoc-${date}.json`, "application/json");
            showToast(`✅ Đã xuất ${selected.length} phiên`);
            closeModal("exportSessionsModal");
          } finally {
            hideLoading();
          }
        });
      function _closeImportGoBack() {
        closeModal("importSessionsModal");
        if (_importCallerModal) {
          const caller = _importCallerModal;
          _importCallerModal = null;
          if (caller === "folderModal") {
            renderFolderModal().then(() => openModal("folderModal"));
          } else {
            openModal("settingsModal");
          }
        }
        // if no caller, just close (don't reopen anything)
      }
      document
        .getElementById("closeImportSessionsModal")
        .addEventListener("click", _closeImportGoBack);
      document
        .getElementById("cancelImportSessionsModal")
        .addEventListener("click", _closeImportGoBack);
      document
        .getElementById("importSessionsModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "importSessionsModal") _closeImportGoBack();
        });
      document
        .getElementById("importSessionsDropZone")
        .addEventListener("click", (e) => {
          if (e.target.tagName !== "INPUT")
            document.getElementById("importSessionsFileInput").click();
        });
      document
        .getElementById("importSessionsDropZone")
        .addEventListener("dragover", (e) => {
          e.preventDefault();
          document
            .getElementById("importSessionsDropZone")
            .classList.add("drag-over");
        });
      document
        .getElementById("importSessionsDropZone")
        .addEventListener("dragleave", () =>
          document
            .getElementById("importSessionsDropZone")
            .classList.remove("drag-over"),
        );
      document
        .getElementById("importSessionsDropZone")
        .addEventListener("drop", (e) => {
          e.preventDefault();
          document
            .getElementById("importSessionsDropZone")
            .classList.remove("drag-over");
          const f = e.dataTransfer.files[0];
          if (f) handleImportSessionsFile(f);
        });
      document
        .getElementById("importSessionsFileInput")
        .addEventListener("change", function () {
          if (this.files[0]) handleImportSessionsFile(this.files[0]);
          requestAnimationFrame(() =>
            requestAnimationFrame(() => (this.value = "")),
          );
        });
      document
        .getElementById("confirmImportSessionsBtn")
        .addEventListener("click", async () => {
          if (!_importSessionsData) return;
          showLoading("Đang nhập...");
          try {
            const result = await importSessionsFromData(_importSessionsData);
            if (!result.ok) {
              showToast("❌ " + result.msg);
              return;
            }
            await renderSessionDropdowns();
            await reloadPracticeList(false);
            const callerAfterImport = _importCallerModal;
            _importCallerModal = null;
            _importSessionsData = null;
            closeModal("importSessionsModal");
            showToast(
              `✅ Nhập ${result.totalSessions} phiên mới, ${result.totalWords} từ`,
              3000,
            );
            if (callerAfterImport === "folderModal") {
              await renderFolderModal();
              openModal("folderModal");
            }
          } finally {
            hideLoading();
          }
        });
      function changeLimit(val) {
        wordLimit = parseInt(val);
        document.getElementById("wordLimitSelect").value = String(wordLimit);
        const wlm = document.getElementById("wordLimitSelectModal");
        if (wlm) wlm.value = String(wordLimit);
        window.batchIdx = 0;
        saveAppState();
        reloadPracticeList(true);
      }
      window.changeLimit = changeLimit;
      document
        .getElementById("wordLimitSelectModal")
        ?.addEventListener("change", (e) => changeLimit(e.target.value));
      document
        .getElementById("wordLimitSelect")
        .addEventListener("change", (e) => changeLimit(e.target.value));
      document
        .getElementById("resetStatsBtn")
        .addEventListener("click", () => {
          window.batchIdx = 0;
          window._shuffledOrder = null;
          reloadPracticeList(true);
          showToast("🔄 Reset");
        });
      document
        .getElementById("mobResetBtn")
        ?.addEventListener("click", () => {
          window.batchIdx = 0;
          window._shuffledOrder = null;
          reloadPracticeList(true);
          showToast("🔄 Reset");
        });
      document
        .getElementById("prevBatchBtn")
        .addEventListener("click", () => goToBatch("prev"));
      document
        .getElementById("nextBatchBtn")
        .addEventListener("click", () => goToBatch("next"));
      document
        .getElementById("selectInBatchBtn")
        .addEventListener("click", openSelectInBatchModal);
      document
        .getElementById("mobileMenuBtn")
        ?.addEventListener("click", (e) => {
          e.stopPropagation();
          unlockTTS();
          if (
            document
              .getElementById("sidebar")
              .classList.contains("mobile-open")
          )
            closeMobileSidebar();
          else openMobileSidebar();
        });
      document
        .getElementById("mobileOverlay")
        .addEventListener("click", closeMobileSidebar);

      function changeExType(val) {
        currentExerciseType = val;
        if (val === "mixedRandom") _mixRound = 0;
        document.getElementById("exerciseTypeSelect").value = val;
        const etm = document.getElementById("exerciseTypeSelectModal");
        if (etm) etm.value = val;
        const mobET = document.getElementById("mobExerciseTypeSelect");
        if (mobET) mobET.value = val;
        stats = { totalAttempts: 0, correctCount: 0 };
        isWaitingForAutoNext = false;
        window._noFocusNext = true;
        renderExercise();
        saveAppState();
        if (exerciseMode === "listen" && currentQuestionsList[currentQIndex])
          speakForMode(currentQuestionsList[currentQIndex]);
      }
      window.changeExType = changeExType;
      document
        .getElementById("exerciseTypeSelect")
        .addEventListener("change", (e) => changeExType(e.target.value));
      document
        .getElementById("exerciseTypeSelectModal")
        ?.addEventListener("change", (e) => changeExType(e.target.value));
      document
        .getElementById("mobExerciseTypeSelect")
        ?.addEventListener("change", (e) => changeExType(e.target.value));
      document
        .getElementById("mobModeSelect")
        ?.addEventListener("change", (e) => {
          exerciseMode = e.target.value;
          localStorage.setItem("exerciseMode", exerciseMode);
          _syncTypeSelectToMode(exerciseMode);
          updateAllToggles();
          renderExercise();
          if (
            exerciseMode === "listen" &&
            currentQuestionsList[currentQIndex]
          )
            speakForMode(currentQuestionsList[currentQIndex]);
          focusAnswerInput();
        });

      const switchTab = (active, show) => {
        ["manualForm", "batchForm", "importFormInModal"].forEach(
          (id) => (document.getElementById(id).style.display = "none"),
        );
        document.getElementById(show).style.display = "";
        ["manualTabBtn", "batchTabBtn", "importTabBtn"].forEach((id) =>
          document.getElementById(id)?.classList.remove("active"),
        );
        document.getElementById(active).classList.add("active");
      };
      document
        .getElementById("manualTabBtn")
        .addEventListener("click", () => {
          switchTab("manualTabBtn", "manualForm");
          setTimeout(
            () => document.getElementById("modalGerman").focus(),
            50,
          );
        });
      document
        .getElementById("batchTabBtn")
        .addEventListener("click", () =>
          switchTab("batchTabBtn", "batchForm"),
        );
      document
        .getElementById("importTabBtn")
        .addEventListener("click", () =>
          switchTab("importTabBtn", "importFormInModal"),
        );

      document
        .getElementById("importDropZoneModal")
        .addEventListener("click", (e) => {
          if (e.target.tagName !== "INPUT")
            document.getElementById("importFileInputModal").click();
        });
      document
        .getElementById("importDropZoneModal")
        .addEventListener("dragover", (e) => {
          e.preventDefault();
          e.currentTarget.style.borderColor = "#58a6ff";
        });
      document
        .getElementById("importDropZoneModal")
        .addEventListener("dragleave", (e) => {
          e.currentTarget.style.borderColor = "#30363d";
        });
      document
        .getElementById("importDropZoneModal")
        .addEventListener("drop", (e) => {
          e.preventDefault();
          e.currentTarget.style.borderColor = "#30363d";
          const f = e.dataTransfer.files[0];
          if (f)
            importFromFile(f, document.getElementById("importStatusModal"));
        });
      document
        .getElementById("importFileInputModal")
        .addEventListener("change", function () {
          const f = this.files[0];
          if (!f) return;
          importFromFile(
            f,
            document.getElementById("importStatusModal"),
            () => setTimeout(() => closeModal("addWordModal"), 1500),
          );
          requestAnimationFrame(() =>
            requestAnimationFrame(() => (this.value = "")),
          );
        });
      ["closeAddModal", "cancelAddModal", "cancelBatchModal"].forEach((id) =>
        document
          .getElementById(id)
          .addEventListener("click", () => closeModal("addWordModal")),
      );
      document
        .getElementById("addWordModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "addWordModal") closeModal("addWordModal");
        });
      document
        .getElementById("confirmAddModal")
        .addEventListener("click", confirmAddManual);
      document
        .getElementById("confirmBatchModal")
        .addEventListener("click", confirmAddBatch);
      document
        .getElementById("modalGerman")
        .addEventListener("keydown", (e) => {
          if (e.key === "Enter")
            document.getElementById("modalWordType").focus();
        });
      document
        .getElementById("modalWordType")
        .addEventListener("keydown", (e) => {
          if (e.key === "Enter")
            document.getElementById("modalMeaning").focus();
        });
      document
        .getElementById("modalMeaning")
        .addEventListener("keydown", (e) => {
          if (e.key === "Enter")
            document.getElementById("modalExample").focus();
        });
      document
        .getElementById("modalExample")
        .addEventListener("keydown", (e) => {
          if (e.key === "Enter") confirmAddManual();
        });
      ["closeEditModal", "cancelEditModal"].forEach((id) =>
        document
          .getElementById(id)
          .addEventListener("click", () => closeModal("editWordModal")),
      );
      document
        .getElementById("editWordModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "editWordModal") closeModal("editWordModal");
        });
      document
        .getElementById("saveEditModal")
        .addEventListener("click", saveEditWord);
      function closeSelectModal() {
        closeModal("selectWordsModal");
        document.getElementById("selectWordsModal").dataset.mode = "global";
      }
      ["closeSelectModal", "cancelSelectModal"].forEach((id) =>
        document
          .getElementById(id)
          .addEventListener("click", closeSelectModal),
      );
      document
        .getElementById("selectWordsModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "selectWordsModal") closeSelectModal();
        });
      document.getElementById("selAllBtn").addEventListener("click", () => {
        document
          .querySelectorAll("#wordGrid .sel-cb:not([disabled])")
          .forEach((cb) => (cb.checked = true));
        updateSelCount(document.querySelectorAll("#wordGrid .sel-cb").length);
      });
      document.getElementById("deselAllBtn").addEventListener("click", () => {
        document
          .querySelectorAll("#wordGrid .sel-cb:not([disabled])")
          .forEach((cb) => (cb.checked = false));
        updateSelCount(document.querySelectorAll("#wordGrid .sel-cb").length);
      });
      document
        .getElementById("applySelectModal")
        .addEventListener("click", async () => {
          const modal = document.getElementById("selectWordsModal"),
            mode = modal.dataset.mode || "global";
          const selected = [
            ...document.querySelectorAll("#wordGrid .sel-cb:checked"),
          ].map((cb) => cb.dataset.id);
          if (!selected.length) {
            showToast("⚠️ Chọn ít nhất 1 từ!");
            return;
          }
          closeSelectModal();
          if (mode === "batch") {
            const batchList = modal._batchList || [],
              selSet = new Set(selected);
            currentQuestionsList = batchList
              .filter((i) => selSet.has(i.id))
              .map((i) => ({ ...i, isAnsweredCorrectly: false }));
            currentQIndex = 0;
            stats = { totalAttempts: 0, correctCount: 0 };
            renderExercise();
            await renderSidebar();
            updateBatchBar(
              isCustomMode && window.customMaster?.length
                ? window.customMaster
                : await buildFullList(),
            );
            saveAppState();
            showToast(
              `✅ Đang học ${currentQuestionsList.length} từ trong batch`,
            );
          } else {
            const fullList = await buildFullList();
            window.customMaster = fullList.filter((i) =>
              selected.includes(i.id),
            );
            window.selectedWordIds = selected;
            isCustomMode = true;
            window.batchIdx = 0;
            let list = [...window.customMaster];
            if (randomMode) list = shuffleArray(list);
            if (wordLimit > 0 && list.length > wordLimit)
              list = list.slice(0, wordLimit);
            currentQuestionsList = list;
            currentQuestionsList.forEach(
              (i) => (i.isAnsweredCorrectly = false),
            );
            currentQIndex = 0;
            stats = { totalAttempts: 0, correctCount: 0 };
            renderExercise();
            await renderSidebar();
            updateBatchBar(window.customMaster);
            saveAppState();
            showToast(`✅ Đang học ${window.customMaster.length} từ đã chọn`);
          }
        });
      ["closeSettingsModal", "closeSettingsDone"].forEach((id) =>
        document
          .getElementById(id)
          .addEventListener("click", () => closeModal("settingsModal")),
      );
      document
        .getElementById("settingsModal")
        .addEventListener("click", (e) => {
          if (e.target.id === "settingsModal") closeModal("settingsModal");
        });
      function getPromptLevel() {
        return document.getElementById("promptLevelSelect")?.value || "A2.2";
      }
      const savedLevel = localStorage.getItem("promptLevel");
      if (savedLevel) {
        const sel = document.getElementById("promptLevelSelect");
        if (sel) sel.value = savedLevel;
      }
      document
        .getElementById("promptLevelSelect")
        ?.addEventListener("change", (e) => {
          localStorage.setItem("promptLevel", e.target.value);
        });
      document
        .getElementById("exportPromptVocabBtn")
        ?.addEventListener("click", async () => {
          const withAnswer = document.getElementById("exportPromptVocabAnswerToggle")?.getAttribute("data-active") === "true";
          await generateAndCopyStudyPrompt("vocab", getPromptLevel(), withAnswer);
        });
      document
        .getElementById("exportPromptGrammarBtn")
        ?.addEventListener("click", async () => {
          const withAnswer = document.getElementById("exportPromptGrammarAnswerToggle")?.getAttribute("data-active") === "true";
          await generateAndCopyStudyPrompt("grammar", getPromptLevel(), withAnswer);
        });
      document
        .getElementById("exportPromptImageBtn")
        ?.addEventListener("click", async () => {
          const withAnswer = document.getElementById("exportPromptImageAnswerToggle")?.getAttribute("data-active") === "true";
          await generateAndCopyStudyPrompt("image", getPromptLevel(), withAnswer);
        });
      document
        .getElementById("exportPromptExamBtn")
        ?.addEventListener("click", async () => {
          const withAnswer = document.getElementById("exportPromptExamAnswerToggle")?.getAttribute("data-active") === "true";
          await generateAndCopyStudyPrompt("exam", getPromptLevel(), withAnswer);
        });
      // Answer toggle inline active/inactive handlers
      (function () {
        const answerToggles = [
          "exportPromptVocabAnswerToggle",
          "exportPromptGrammarAnswerToggle",
          "exportPromptTranslationAnswerToggle",
          "exportPromptImageAnswerToggle",
          "exportPromptExamAnswerToggle",
        ];
        answerToggles.forEach((id) => {
          const btn = document.getElementById(id);
          if (!btn) return;
          btn.addEventListener("click", () => {
            const active = btn.getAttribute("data-active") === "true";
            btn.setAttribute("data-active", String(!active));
            if (!active) {
              btn.style.background = "rgba(88,166,255,0.15)";
              btn.style.borderColor = "#58a6ff";
              btn.style.color = "#58a6ff";
            } else {
              btn.style.background = "transparent";
              btn.style.borderColor = "var(--border2)";
              btn.style.color = "var(--tx3)";
            }
          });
        });
      })();
      document
        .getElementById("exportPromptSprechenT2Btn")
        ?.addEventListener("click", async () => {
          await generateAndCopyStudyPrompt("sprechen-t2", getPromptLevel());
        });
      document
        .getElementById("exportPromptSprechenT3Btn")
        ?.addEventListener("click", async () => {
          await generateAndCopyStudyPrompt("sprechen-t3", getPromptLevel());
        });
      document
        .getElementById("exportPromptTranslationBtn")
        ?.addEventListener("click", async () => {
          const withAnswer = document.getElementById("exportPromptTranslationAnswerToggle")?.getAttribute("data-active") === "true";
          await generateAndCopyStudyPrompt("translation", getPromptLevel(), withAnswer);
        });
      document.getElementById("tgDark").addEventListener("click", () => {
        isDarkMode = !isDarkMode;
        localStorage.setItem("darkMode", isDarkMode);
        applyDark();
        updateAllToggles();
        showToast(isDarkMode ? "🌙 Tối" : "☀️ Sáng");
      });
      document.getElementById("tgRandom").addEventListener("click", () => {
        randomMode = !randomMode;
        localStorage.setItem("randomMode", randomMode);
        window._shuffledOrder = null;
        updateAllToggles();
        reloadPracticeList(true);
        showToast(randomMode ? "Random: BẬT" : "Random: TẮT");
      });
      document.getElementById("tgAutoAdv").addEventListener("click", () => {
        autoAdvanceOnCorrect = !autoAdvanceOnCorrect;
        localStorage.setItem("autoAdvanceOnCorrect", autoAdvanceOnCorrect);
        updateAllToggles();
        showToast(
          autoAdvanceOnCorrect ? "⚡ Auto-next: BẬT" : "⏸️ Auto-next: TẮT",
        );
      });
      document.getElementById("tgSpeak").addEventListener("click", () => {
        soundEnabled = !soundEnabled;
        localStorage.setItem("soundEnabled", soundEnabled);
        updateAllToggles();
        showToast(soundEnabled ? "🔊 Âm thanh: BẬT" : "🔇 Âm thanh: TẮT");
      });
      document.getElementById("tgStudy").addEventListener("click", () => {
        studyMode = !studyMode;
        localStorage.setItem("studyMode", studyMode);
        if (studyMode) {
          autoAdvanceOnCorrect = false;
          localStorage.setItem("autoAdvanceOnCorrect", false);
        }
        updateAllToggles();
        showToast(studyMode ? "Học bài: BẬT" : "Học bài: TẮT", 2200);
      });
      {
        const fsSel = document.getElementById("folderSortSelect");
        if (fsSel) {
          fsSel.value = folderSortMode;
          fsSel.addEventListener("change", async () => {
            folderSortMode = fsSel.value;
            localStorage.setItem("folderSortMode", folderSortMode);
            await renderFolderModal();
          });
        }
      }
      document
        .getElementById("tgFolderMergeMode")
        .addEventListener("click", async () => {
          folderMergeSelectMode = !folderMergeSelectMode;
          updateAllToggles();
          if (!folderMergeSelectMode) {
            // Tắt "Gộp phiên" → huỷ chọn tất cả phiên đã tích, quay về phiên hiện tại
            mergedSessionIds = [];
            currentSource = "session";
            document.getElementById("sourceSelect").value = "session";
            const sModal = document.getElementById("sourceSelectModal");
            if (sModal) sModal.value = "session";
            window.customMaster = null;
            window.customFilterCriteria = null;
            window.selectedWordIds = null;
            window.batchIdx = 0;
            isCustomMode = false;
            saveAppState();
            showLoading("");
            try {
              await reloadPracticeList(true);
            } finally {
              hideLoading();
            }
            showToast("📁 Đã tắt gộp phiên, về phiên hiện tại", 2000);
          } else {
            showToast("✅ Có thể chọn phiên để gộp", 2000);
          }
          await renderFolderModal();
        });
      document
        .getElementById("tgOnlyUnmastered")
        .addEventListener("click", () => {
          onlyUnmastered = !onlyUnmastered;
          localStorage.setItem("onlyUnmastered", onlyUnmastered);
          updateAllToggles();
          window.batchIdx = 0;
          reloadPracticeList(true);
          showToast(
            onlyUnmastered ? "🎯 Chỉ học từ chưa thuộc" : "📋 Học tất cả từ",
            2200,
          );
        });

      document.getElementById("tgStrictVocab")?.addEventListener("click", () => {
        strictVocabCheck = !strictVocabCheck;
        localStorage.setItem("strictVocabCheck", strictVocabCheck);
        updateAllToggles();
        showToast(strictVocabCheck ? "✅ Kiểm tra 100%: BẬT" : "🔓 Kiểm tra rút gọn: BẬT", 2200);
      });

      document.getElementById("tgBreakEnabled")?.addEventListener("click", () => {
        breakEnabled = !breakEnabled;
        localStorage.setItem("breakEnabled", breakEnabled);
        updateAllToggles();
        restartBreakCycle();
        showToast(
          breakEnabled ? "🐱 Nhắc nghỉ: BẬT" : "🐱 Nhắc nghỉ: TẮT",
          2200,
        );
      });
      document.getElementById("breakWorkMinutes")?.addEventListener("change", (e) => {
        let v = parseInt(e.target.value, 10);
        if (!v || v < 1) v = 1;
        if (v > 180) v = 180;
        breakWorkMinutes = v;
        e.target.value = v;
        localStorage.setItem("breakWorkMinutes", breakWorkMinutes);
        restartBreakCycle();
        showToast(`⏱️ Làm việc: ${v} phút`, 1800);
      });
      document.getElementById("breakRestMinutes")?.addEventListener("change", (e) => {
        let v = parseInt(e.target.value, 10);
        if (!v || v < 1) v = 1;
        if (v > 60) v = 60;
        breakRestMinutes = v;
        e.target.value = v;
        localStorage.setItem("breakRestMinutes", breakRestMinutes);
        showToast(`☕ Nghỉ: ${v} phút`, 1800);
      });

      // 3 nút mode trong settings
      ["tgModeWrite", "tgModeChoose", "tgModeListen"].forEach((btnId) => {
        document.getElementById(btnId)?.addEventListener("click", () => {
          exerciseMode =
            btnId === "tgModeWrite"
              ? "write"
              : btnId === "tgModeChoose"
                ? "choose"
                : "listen";
          localStorage.setItem("exerciseMode", exerciseMode);
          _syncTypeSelectToMode(exerciseMode);
          updateAllToggles();
          renderExercise();
          const label =
            exerciseMode === "choose"
              ? "Dạng bài: Chọn"
              : exerciseMode === "listen"
                ? "Dạng bài: Nghe"
                : "Dạng bài: Viết";
          showToast(label);
          focusAnswerInput();
        });
      });

      // ── Bulk mode ──
      const bulkSelected = new Set();
      let bulkMode = false;
      let bulkPendingAction = null; // 'master'|'unmaster'|'flag'|'unflag'|'delete'
      window._bulkSelected = bulkSelected;
      window._bulkMode = () => bulkMode;
      window._updateBulkCount = () => {
        const lbl = document.getElementById("bulkCountLabel");
        if (lbl) lbl.textContent = bulkSelected.size + " từ";
        const cbs = document.querySelectorAll(".word-item-cb");
        const allChecked = cbs.length > 0 && [...cbs].every((cb) => bulkSelected.has(cb.dataset.id));
        const btn = document.getElementById("bulkSelectAllBtn");
        if (btn) btn.innerHTML = allChecked
          ? '<i class="fa-solid fa-square-check"></i>'
          : '<i class="fa-regular fa-square"></i>';
      };

      // Dropdown toggle
      const bulkDropdown = document.getElementById("bulkDropdown");
      document.getElementById("bulkModeBtn").addEventListener("click", (e) => {
        e.stopPropagation();
        if (bulkMode) { exitBulkMode(); return; }
        const isOpen = bulkDropdown.style.display !== "none";
        bulkDropdown.style.display = isOpen ? "none" : "block";
      });
      // Close dropdown on outside click
      document.addEventListener("click", () => {
        bulkDropdown.style.display = "none";
      });
      bulkDropdown.addEventListener("click", (e) => e.stopPropagation());

      // When a dropdown action is chosen → enter selection mode
      const actionLabels = {
        master: '<i class="fa-solid fa-circle-check"></i> Đánh dấu thuộc',
        unmaster: '<i class="fa-solid fa-rotate-left"></i> Bỏ thuộc lòng',
        flag: '<i class="fa-solid fa-star"></i> Đánh dấu chú ý',
        unflag: '<i class="fa-regular fa-star"></i> Bỏ chú ý',
        delete: '<i class="fa-solid fa-trash"></i> Xóa từ'
      };
      document.querySelectorAll(".bulk-dropdown-item").forEach(item => {
        item.addEventListener("click", () => {
          bulkDropdown.style.display = "none";
          bulkPendingAction = item.dataset.action;
          const lbl = document.getElementById("bulkActionLabel");
          if (lbl) lbl.innerHTML = actionLabels[bulkPendingAction] || "";
          enterBulkMode();
        });
      });

      function enterBulkMode() {
        bulkMode = true;
        document.body.classList.add("bulk-mode");
        document.getElementById("sidebarBulkBar").classList.remove("hidden");
        document.getElementById("sidebarBulkBar").classList.add("flex");
        bulkSelected.clear();
        window._updateBulkCount();
        const selAllBtn = document.getElementById("bulkSelectAllBtn");
        if (selAllBtn) {
          selAllBtn.innerHTML = '<i class="fa-regular fa-square"></i>';
          selAllBtn.style.display = currentSource === "merged" ? "none" : "";
        }
        renderSidebar();
      }
      function exitBulkMode() {
        bulkMode = false;
        bulkPendingAction = null;
        document.body.classList.remove("bulk-mode");
        document.getElementById("sidebarBulkBar").classList.add("hidden");
        document.getElementById("sidebarBulkBar").classList.remove("flex");
        bulkSelected.clear();
        renderSidebar();
      }
      document
        .getElementById("bulkCancelBtn")
        .addEventListener("click", exitBulkMode);

      // Confirm button dispatches to the right hidden action button
      document.getElementById("bulkConfirmBtn").addEventListener("click", () => {
        if (!bulkPendingAction) return;
        const map = {
          master: "bulkMasterBtn",
          unmaster: "bulkUnmasterBtn",
          flag: "bulkFlagBtn",
          unflag: "bulkUnflagBtn",
          delete: "bulkDeleteBtn"
        };
        const targetId = map[bulkPendingAction];
        if (targetId) document.getElementById(targetId).click();
      });
      document
        .getElementById("bulkSelectAllBtn")
        .addEventListener("click", async () => {
          const all = await buildFullListAll();
          const allIds = all.map((v) => v.id);
          const allSelected = allIds.every((id) => bulkSelected.has(id));
          if (allSelected) {
            // Bỏ chọn tất cả
            bulkSelected.clear();
            document.getElementById("bulkSelectAllBtn").innerHTML = '<i class="fa-regular fa-square"></i>';
          } else {
            // Chọn tất cả
            allIds.forEach((id) => bulkSelected.add(id));
            document.getElementById("bulkSelectAllBtn").innerHTML = '<i class="fa-solid fa-square-check"></i>';
          }
          window._updateBulkCount();
          // Sync checkboxes in DOM
          document.querySelectorAll(".word-item-cb").forEach((cb) => {
            const isChecked = bulkSelected.has(cb.dataset.id);
            cb.checked = isChecked;
            const circle = cb.parentElement?.querySelector('.word-item-circle[data-id="' + cb.dataset.id + '"]');
            if (circle) circle.classList.toggle('checked', isChecked);
          });
        });
      document
        .getElementById("bulkMasterBtn")
        .addEventListener("click", async () => {
          if (!bulkSelected.size) {
            showToast("⚠️ Chưa chọn từ nào");
            return;
          }
          const all = await buildFullListAll();
          for (const id of bulkSelected) {
            const w = all.find((v) => v.id === id);
            if (!w || w.isMastered) continue;
            const sessId = w._sessId || currentSessionId,
              realId = w._realId || w.id;
            await dbMarkMastered(sessId, realId);
            await dbUnmarkFlagged(sessId, realId);
            if (sessId === currentSessionId) {
              masteredIds.add(realId);
              flaggedIds.delete(realId);
            }
          }
          currentQuestionsList = currentQuestionsList.filter(
            (q) => !bulkSelected.has(q.id),
          );
          if (window.customMaster)
            window.customMaster = window.customMaster.filter(
              (q) => !bulkSelected.has(q.id),
            );
          showToast(`✅ Đã đánh dấu ${bulkSelected.size} từ thuộc lòng`);
          exitBulkMode();
          renderExercise();
          await renderSidebar();
        });
      document
        .getElementById("bulkUnmasterBtn")
        .addEventListener("click", async () => {
          if (!bulkSelected.size) {
            showToast("⚠️ Chưa chọn từ nào");
            return;
          }
          const all = await buildFullListAll();
          for (const id of bulkSelected) {
            const w = all.find((v) => v.id === id);
            if (!w) continue;
            const sessId = w._sessId || currentSessionId,
              realId = w._realId || w.id;
            await dbUnmarkMastered(sessId, realId);
            if (sessId === currentSessionId) masteredIds.delete(realId);
          }
          showToast(`↩️ Đã đưa ${bulkSelected.size} từ về danh sách học`);
          exitBulkMode();
          await reloadPracticeList(false);
        });
      document
        .getElementById("bulkFlagBtn")
        .addEventListener("click", async () => {
          if (!bulkSelected.size) {
            showToast("⚠️ Chưa chọn từ nào");
            return;
          }
          const all = await buildFullListAll();
          for (const id of bulkSelected) {
            const w = all.find((v) => v.id === id);
            if (!w || w.isFlagged) continue;
            const sessId = w._sessId || currentSessionId,
              realId = w._realId || w.id;
            await dbMarkFlagged(sessId, realId);
            await dbUnmarkMastered(sessId, realId);
            if (sessId === currentSessionId) {
              flaggedIds.add(realId);
              masteredIds.delete(realId);
            }
          }
          showToast(`⭐ Đã đánh dấu chú ý ${bulkSelected.size} từ`);
          exitBulkMode();
          renderExercise();
          await renderSidebar();
        });
      document
        .getElementById("bulkUnflagBtn")
        .addEventListener("click", async () => {
          if (!bulkSelected.size) {
            showToast("⚠️ Chưa chọn từ nào");
            return;
          }
          const all = await buildFullListAll();
          for (const id of bulkSelected) {
            const w = all.find((v) => v.id === id);
            if (!w) continue;
            const sessId = w._sessId || currentSessionId,
              realId = w._realId || w.id;
            await dbUnmarkFlagged(sessId, realId);
            if (sessId === currentSessionId) flaggedIds.delete(realId);
          }
          showToast(`☆ Đã bỏ chú ý ${bulkSelected.size} từ`);
          exitBulkMode();
          renderExercise();
          await renderSidebar();
        });
      document
        .getElementById("bulkDeleteBtn")
        .addEventListener("click", async () => {
          if (!bulkSelected.size) {
            showToast("⚠️ Chưa chọn từ nào");
            return;
          }
          if (!(await customConfirm(`Xóa ${bulkSelected.size} từ đã chọn?`))) return;
          showLoading("Đang xóa...");
          try {
            for (const id of bulkSelected) {
              const realId = id.includes(":") ? id.split(":").slice(1).join(":") : id;
              const sessId = id.includes(":") ? id.split(":")[0] : currentSessionId;
              await dbDeleteWord(sessId, realId);
              try { await dbUnmarkMastered(sessId, realId); } catch (_) { }
              try { await dbUnmarkFlagged(sessId, realId); } catch (_) { }
              masteredIds.delete(realId);
              flaggedIds.delete(realId);
            }
            currentQuestionsList = currentQuestionsList.filter(
              (q) => !bulkSelected.has(q.id),
            );
            if (window.customMaster)
              window.customMaster = window.customMaster.filter(
                (q) => !bulkSelected.has(q.id),
              );
            if (window.selectedWordIds)
              window.selectedWordIds = window.selectedWordIds.filter(
                (i) => !bulkSelected.has(i),
              );
            showToast(`🗑 Đã xóa ${bulkSelected.size} từ`);
            exitBulkMode();
            await renderSessionDropdowns();
            renderExercise();
            await renderSidebar();
          } catch (err) {
            console.error("Bulk delete error:", err);
            if (err?.code === "permission-denied" || err?.message?.includes("permissions")) {
              showToast("❌ Không có quyền xóa. Vui lòng đăng nhập lại.");
            } else {
              showToast("❌ Lỗi khi xóa: " + (err?.message || err));
            }
          } finally {
            hideLoading();
          }
        });

      // TTS unlock on first interaction
      const unlockOnce = () => {
        unlockTTS();
        window.speechSynthesis?.getVoices();
      };
      document.addEventListener("touchstart", unlockOnce, {
        once: true,
        passive: true,
      });
      document.addEventListener("touchend", unlockOnce, {
        once: true,
        passive: true,
      });
      document.addEventListener("click", unlockOnce, { once: true });
      document.addEventListener("keydown", unlockOnce, { once: true });

      document
        .getElementById("mobPrevBatchBtn")
        ?.addEventListener("click", () => goToBatch("prev"));
      document
        .getElementById("mobNextBatchBtn")
        ?.addEventListener("click", () => goToBatch("next"));
      document
        .getElementById("mobSelectInBatchBtn")
        ?.addEventListener("click", () => openSelectInBatchModal());
      // Modebar mode dropdown
      document
        .getElementById("modebarModeSelect")
        ?.addEventListener("change", (e) => {
          exerciseMode = e.target.value;
          localStorage.setItem("exerciseMode", exerciseMode);
          _syncTypeSelectToMode(exerciseMode);
          updateAllToggles();
          renderExercise();
          if (
            exerciseMode === "listen" &&
            currentQuestionsList[currentQIndex]
          )
            speakForMode(currentQuestionsList[currentQIndex]);
          showToast(
            exerciseMode === "choose"
              ? "Chọn"
              : exerciseMode === "listen"
                ? "Nghe"
                : "Viết",
          );
          focusAnswerInput();
        });

      ["floatModeWrite", "floatModeChoose", "floatModeListen"].forEach(
        (id) => {
          document.getElementById(id)?.addEventListener("click", () => {
            exerciseMode =
              id === "floatModeWrite"
                ? "write"
                : id === "floatModeChoose"
                  ? "choose"
                  : "listen";
            localStorage.setItem("exerciseMode", exerciseMode);
            _syncTypeSelectToMode(exerciseMode);
            updateAllToggles();
            updateModeFloatBar();
            renderExercise();
            showToast(
              exerciseMode === "choose"
                ? "Chọn"
                : exerciseMode === "listen"
                  ? "Nghe"
                  : "Viết",
            );
            focusAnswerInput();
          });
        },
      );
      updateModeFloatBar();
    }

    async function createNewSession() {
      const name = await customPrompt("Tên phiên mới:");
      if (!name) return;
      const ns = { id: "sess_" + Date.now(), name, createdAt: Date.now() };
      showLoading("Đang tạo phiên...");
      try {
        await dbSaveSession(ns);
        currentSessionId = ns.id;
        window.customMaster = null;
        window.customFilterCriteria = null;
        window.selectedWordIds = null;
        window.batchIdx = 0;
        isCustomMode = false;
        masteredIds = new Set();
        flaggedIds = new Set();
        await renderSessionDropdowns();
        await reloadPracticeList(true);
      } finally {
        hideLoading();
      }
    }
    async function switchSession(id) {
      currentSessionId = id;
      window.customMaster = null;
      window.customFilterCriteria = null;
      window.selectedWordIds = null;
      window.batchIdx = 0;
      isCustomMode = false;
      showLoading("Đang tải phiên...");
      try {
        masteredIds = await dbGetMastered(currentSessionId);
        flaggedIds = await dbGetFlagged(currentSessionId);
        // Switch real-time listener to new session
        if (window._onRemoteUpdate)
          switchRealtimeSession(currentSessionId, window._onRemoteUpdate);
        saveAppState();
        if (currentSource === "session") await reloadPracticeList(true);
        await renderSidebar();
        await renderSessionDropdowns();
      } finally {
        hideLoading();
      }
    }

    async function bootstrap() {
      showLoading("Đang xác thực...");
      await _authReady; // Chờ Firebase Auth xác nhận user trước khi làm gì
      showLoading("Đang khởi động...");
      try {
        // Clear stale vocab caches so onSnapshot always has fresh data
        Object.keys(localStorage)
          .filter((k) => k.startsWith("vocab_") || k.startsWith("cache_mastered_") || k.startsWith("cache_flagged_"))
          .forEach((k) => localStorage.removeItem(k));
        loadSettings();
        applyDark();
        if (localStorage.getItem("darkMode") === null) {
          isDarkMode = !window.matchMedia("(prefers-color-scheme: light)")
            .matches;
          applyDark();
        }
        let sessions = await dbGetAllSessions();
        if (!sessions.length) {
          const def = {
            id: "sess_" + Date.now(),
            name: "📘 Mặc định",
            createdAt: Date.now(),
          };
          await dbSaveSession(def);
          await dbAddWord(
            def.id,
            {
              id: uid(),
              originalGerman: "der Tisch",
              mainGerman: "der Tisch",
              meaning: "cái bàn",
              wordType: "n",
              example: "Der Tisch ist groß.",
            },
            0,
          );
          sessions = [def];
        }
        const saved = loadAppState();
        const validSession =
          saved && sessions.find((s) => s.id === saved.sessionId);
        currentSessionId = validSession ? saved.sessionId : sessions[0].id;
        if (saved && validSession) {
          currentSource = saved.source || "session";
          currentExerciseType = saved.exerciseType || "fullWord";
          wordLimit = saved.wordLimit || 0;
          window.batchIdx = saved.batchIdx || 0;
          isCustomMode = saved.isCustomMode || false;
          window.selectedWordIds = saved.selectedWordIds || null;
          stats = saved.stats || { totalAttempts: 0, correctCount: 0 };
          sidebarFilterTab = saved.sidebarFilterTab || "all";
          sidebarScope = saved.sidebarScope || "current";
          mergedSessionIds = saved.mergedSessionIds || [];
          exerciseMode = saved.exerciseMode || exerciseMode;
          // Khôi phục trạng thái toggle "Gộp phiên": trước đây folderMergeSelectMode
          // luôn reset về false sau khi tải lại trang, khiến toggle hiển thị TẮT dù
          // đang thực sự học ở chế độ gộp phiên (currentSource === "merged").
          folderMergeSelectMode =
            currentSource === "merged" || mergedSessionIds.length > 0;
          // Migrate old listenWrite
          if (currentExerciseType === "listenWrite") {
            currentExerciseType = "fullWord";
            exerciseMode = "listen";
          }
        }
        masteredIds = await dbGetMastered(currentSessionId);
        flaggedIds = await dbGetFlagged(currentSessionId);

        // ── Real-time sync callback (only fires for changes from other devices) ──
        const _onRemoteUpdate = async (type, sid) => {
          if (sid !== currentSessionId && currentSource !== "merged") return;

          // Update top-level refs — only for the active session to avoid corrupting global state
          if (type === "mastered" && sid === currentSessionId && _cache.mastered[sid])
            masteredIds = _cache.mastered[sid];
          if (type === "flagged" && sid === currentSessionId && _cache.flagged[sid])
            flaggedIds = _cache.flagged[sid];

          clearTimeout(_onRemoteUpdate._t);
          _onRemoteUpdate._t = setTimeout(async () => {
            if (type === "vocab") {
              // Vocab changed remotely (word added/deleted/edited on another device)
              // → cập nhật tại chỗ, giữ nguyên thứ tự và vị trí câu hỏi hiện tại
              // Nếu chính mình vừa sửa từ (saveEditWord), bỏ qua để tránh cập nhật trùng
              if (window._suppressRemoteReload) return;
              await mergeVocabChangesInPlace();
              await renderSidebar();
              await renderSessionDropdowns();
            } else {
              // Mastered/flagged changed — only re-render sidebar (counts/badges)
              // Don't reset the current exercise question
              await renderSidebar();
            }
          }, 400);
        };
        initRealtimeSync(currentSessionId, _onRemoteUpdate);
        if (currentSource === "merged" && mergedSessionIds.length)
          mergedSessionIds.forEach((sid) => addRealtimeSession(sid, _onRemoteUpdate));
        window._onRemoteUpdate = _onRemoteUpdate;

        document.getElementById("sourceSelect").value = currentSource;
        document.getElementById("wordLimitSelect").value = String(wordLimit);
        document.getElementById("exerciseTypeSelect").value =
          currentExerciseType;
        const _etmInit = document.getElementById("exerciseTypeSelectModal");
        if (_etmInit) _etmInit.value = currentExerciseType;
        const sfs = document.getElementById("sidebarFilterSelect");
        if (sfs) sfs.value = sidebarFilterTab;
        updateAllToggles();
        initEvents();
        initKeyboard();
        await renderSessionDropdowns();
        if (
          saved &&
          validSession &&
          isCustomMode &&
          window.selectedWordIds?.length
        ) {
          const fullList = await buildFullList();
          window.customMaster = fullList.filter((i) =>
            window.selectedWordIds.includes(i.id),
          );
        }
        const snapshot =
          saved && validSession && saved.questionsList?.length
            ? saved.questionsList
            : null;
        await reloadPracticeList(false, snapshot);
        if (saved && validSession) {
          const qi = saved.qIndex || 0;
          if (qi < currentQuestionsList.length) {
            currentQIndex = qi;
            if (currentQuestionsList[currentQIndex])
              currentQuestionsList[currentQIndex].isAnsweredCorrectly = false;
          }
          renderExercise();
          await renderSidebar();
          // reloadPracticeList() ở trên đã tự saveAppState() với qIndex = 0 (trước khi
          // currentQIndex được khôi phục về qi ở đây) → lưu lại state đúng để lần load
          // trang kế tiếp không bị reset về câu hỏi đầu tiên (đặc biệt rõ khi bật random).
          saveAppState();
        }
        startBreakWorkTimer();
      } finally {
        hideLoading();
      }
    }
    bootstrap().catch((err) => {
      console.error("Bootstrap failed:", err);
      hideLoading();
    });
