/**
 * Brain.fm Console Data Extractor (Lean Version with Pause)
 */

(function() {
    'use strict';

    const collectedTracks = new Map();
    let extractionInterval = null;
    let currentTrackName = null;
    let isPaused = false;
    let repetitionCount = 0;
    let consecutiveRepetitions = 0;
    
    function createUI() {
        const container = document.createElement('div');
        container.id = 'brainFmExtractor';
        container.style.cssText = `
            position: fixed; bottom: max(16px, env(safe-area-inset-bottom)); right: max(16px, env(safe-area-inset-right)); width: 320px;
            background: #121212; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            color: #fff; font-family: sans-serif; z-index: 9999;
            border: 1px solid rgba(255,255,255,0.1);
        `;

        container.innerHTML = `
            <div style="padding: 12px 16px; border-bottom: 1px solid rgba(255,255,255,0.1); display: flex; justify-content: space-between; align-items: center;">
                <div style="font-weight: bold; color: #fff;">Brain.fm Extractor</div>
                <div id="extStatus" style="font-size: 11px; padding: 3px 8px; background: #3498db; color: #fff; border-radius: 4px; font-weight: bold;">Active</div>
            </div>
            <div style="padding: 16px;">
                <div style="margin-bottom: 12px; color: #757575; font-size: 13px;">
                    Tracks: <b id="extCount" style="font-size: 18px; color: #fff; margin-left: 4px; margin-right: 12px;">0</b>
                    Repeats: <b id="extRepeats" style="font-size: 18px; color: #f1c40f; margin-left: 4px;">0</b>
                </div>
                <div style="margin-bottom: 16px; font-size: 13px; color: #757575;">Current: <span id="extCurrent" style="color: #fff; display: block; margin-top: 4px; font-weight: 500;">Waiting...</span></div>
                <div style="display: flex; gap: 8px;">
                    <button id="extPauseBtn" style="flex: 1; padding: 8px; cursor: pointer; background: #fff; color: #121212; border: none; border-radius: 4px; font-weight: bold;">Pause</button>
                    <button id="extExportBtn" style="flex: 1; padding: 8px; cursor: pointer; background: #07bc0c; color: #fff; border: none; border-radius: 4px; font-weight: bold;">Export</button>
                </div>
            </div>
        `;
        document.body.appendChild(container);

        document.getElementById('extPauseBtn').onclick = () => {
            isPaused = !isPaused;
            document.getElementById('extPauseBtn').textContent = isPaused ? 'Resume' : 'Pause';
            document.getElementById('extStatus').textContent = isPaused ? 'Paused' : 'Active';
            document.getElementById('extStatus').style.background = isPaused ? '#f1c40f' : '#3498db';
        };
        document.getElementById('extExportBtn').onclick = exportData;
    }

    function extractTrackData() {
        const data = {};
        try {
            const titleElem = document.querySelector('[data-testid="currentTrackTitle"]');
            data.song_name = titleElem ? titleElem.textContent.trim() : null;

            const genreElem = document.querySelector('[data-testid="trackGenre"]');
            data.genre = genreElem ? genreElem.textContent.replace(/\d+\s*BPM/i, '').trim() : null;

            const neuralElem = document.querySelector('[data-testid="trackNeuralEffect"]');
            data.neural_effect = neuralElem ? neuralElem.textContent.trim() : null;

            const activityElem = document.querySelector('[data-testid="selectedActivity"]');
            data.sub_activity = activityElem ? activityElem.textContent.trim() : null;

            const coverElem = document.querySelector('.sc-f8a69023-8.emiyWf img, img.sc-f8a69023-9.bPLXa-d');
            data.cover_url = coverElem ? coverElem.src : null;

            data.activity = null;
            data.moods = null;
            data.instrumentation = null;
            data.complexity = null;
            data.brightness = null;

            // Hardcode exactly to the requested HTML snippet structure
            const labelClass = 'sc-681eeceb-0 sc-681eeceb-1 sc-2935bf0f-2 CJqWw cVfLuA duksNq';
            const textValueClass = 'sc-681eeceb-0 sc-681eeceb-2 sc-2935bf0f-3 CJqWw bMnsXL cHgexo';
            const levelValueClass = 'sc-2935bf0f-17 hRuZwN';

            const labelSelectors = labelClass.split(' ').map(c => `.${c}`).join('');
            const textValueSelectors = textValueClass.split(' ').map(c => `.${c}`).join('');
            const levelValueSelectors = levelValueClass.split(' ').map(c => `.${c}`).join('');

            const allLabels = document.querySelectorAll(labelSelectors);

            for (let i = 0; i < allLabels.length; i++) {
                const labelElem = allLabels[i];
                const text = labelElem.textContent.trim();
                const nextElem = labelElem.nextElementSibling;
                
                if (!nextElem) continue;

                // Match text fields
                if (nextElem.matches(textValueSelectors)) {
                    const value = nextElem.textContent.trim();
                    if (text === 'Mental State') data.activity = value;
                    if (text === 'Activity') data.sub_activity = value;
                    if (text === 'Moods') data.moods = value;
                    if (text === 'Instrumentation') data.instrumentation = value;
                }

                // Match level fields
                if (nextElem.matches(levelValueSelectors)) {
                    const value = nextElem.textContent.trim();
                    if (text === 'Complexity') data.complexity = value;
                    if (text === 'Brightness') data.brightness = value;
                }
            }

            const audioElem = document.querySelector('audio[preload="auto"]');
            if (audioElem && audioElem.src) {
                data.url = audioElem.src;
            }

            return data.song_name ? data : null;
        } catch (e) {
            return null;
        }
    }

    function showToast(message, type = 'info') {
        let toastContainer = document.getElementById('bfm-toast-container');
        if (!toastContainer) {
            toastContainer = document.createElement('div');
            toastContainer.id = 'bfm-toast-container';
            toastContainer.style.cssText = `
                position: fixed; top: max(16px, env(safe-area-inset-top)); right: max(16px, env(safe-area-inset-right));
                z-index: 99999; display: flex; flex-direction: column; gap: 10px; pointer-events: none;
            `;
            document.body.appendChild(toastContainer);
        }

        const colors = { success: '#07bc0c', warning: '#f1c40f', error: '#e74c3c', info: '#3498db' };
        const toast = document.createElement('div');
        toast.style.cssText = `
            background: #121212; color: #fff; border-left: 4px solid ${colors[type] || colors.info};
            padding: 12px 16px; border-radius: 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            font-family: sans-serif; font-size: 13px; min-width: 250px; line-height: 1.4;
            opacity: 0; transform: translateX(100%); transition: all 0.3s ease;
        `;
        toast.innerHTML = message;
        toastContainer.appendChild(toast);

        requestAnimationFrame(() => {
            toast.style.opacity = '1';
            toast.style.transform = 'translateX(0)';
        });

        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    function checkTrack() {
        if (isPaused) return;

        // Auto-click the "+ Details" button if it's visible, to expose the hidden data fields in the DOM
        const detailBtn = document.querySelector('[data-testid="trackDetail"]');
        if (detailBtn && detailBtn.textContent.includes('+ Details')) {
            detailBtn.click();
            // Wait 500ms for React to render details before running extraction
            setTimeout(processTrack, 500);
            return;
        }

        processTrack();
    }

    function processTrack() {
        if (isPaused) return;
        const data = extractTrackData();
        if (!data || !data.song_name) return;

        if (data.song_name !== currentTrackName) {
            currentTrackName = data.song_name;
            if (collectedTracks.has(data.song_name)) {
                repetitionCount++;
                consecutiveRepetitions++;
                document.getElementById('extRepeats').textContent = repetitionCount;
                
                // Stop automatically and trigger export if repetitions match collected tracks (all songs covered)
                if (repetitionCount >= collectedTracks.size) {
                    isPaused = true;
                    if (extractionInterval) {
                        clearInterval(extractionInterval);
                        extractionInterval = null;
                    }
                    document.getElementById('extStatus').textContent = `Done (${collectedTracks.size} tracks)`;
                    document.getElementById('extStatus').style.background = '#07bc0c';
                    showToast(`<b>Finished!</b> All ${collectedTracks.size} tracks collected.`, 'success');
                    exportData();
                    return;
                }

                showToast(`<b>Repeated:</b> ${data.song_name}`, 'warning');
                const skipBtn = document.querySelector('[data-testid="skipButton"]');
                if (skipBtn) setTimeout(() => skipBtn.click(), 500);
            } else {
                consecutiveRepetitions = 0;
                collectedTracks.set(data.song_name, data);
                document.getElementById('extCount').textContent = collectedTracks.size;
                document.getElementById('extCurrent').textContent = data.song_name;
                
                showToast(`<b>Added:</b> ${data.song_name}`, 'success');
                const skipBtn = document.querySelector('[data-testid="skipButton"]');
                if (skipBtn) setTimeout(() => skipBtn.click(), 500);
            }
        }
    }

    function exportData() {
        if (collectedTracks.size === 0) return alert('No tracks');
        const exportData = Array.from(collectedTracks.values());
        
        let activity = 'Unknown';
        let subActivity = 'Unknown';
        if (exportData.length > 0) {
            activity = exportData[0].activity || activity;
            activity = activity.charAt(0).toUpperCase() + activity.slice(1);
            subActivity = exportData[0].sub_activity || subActivity;
        }

        const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${activity} - ${subActivity}.json`;
        a.click();
    }

    function init() {
        const existing = document.getElementById('brainFmExtractor');
        if (existing) existing.remove();
        createUI();
        extractionInterval = setInterval(checkTrack, 2000);
    }

    init();
})();
