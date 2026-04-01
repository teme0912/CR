(function () {
    const storageKey = "portal_theme";
    const body = document.body;
    if (!body) {
        return;
    }

    function detectSystemTheme() {
        return window.matchMedia("(prefers-color-scheme: dark)").matches ? "night" : "day";
    }

    function applyTheme(theme) {
        const resolved = theme === "day" ? "day" : "night";
        body.classList.remove("theme-day", "theme-night");
        body.classList.add(resolved === "day" ? "theme-day" : "theme-night");
        return resolved;
    }

    function getStoredTheme() {
        return localStorage.getItem(storageKey);
    }

    function setStoredTheme(theme) {
        if (theme === "day" || theme === "night") {
            localStorage.setItem(storageKey, theme);
        }
    }

    window.__setPortalTheme = function (theme) {
        const resolved = applyTheme(theme);
        setStoredTheme(resolved);
        return resolved;
    };

    window.__getPortalTheme = function () {
        return body.classList.contains("theme-day") ? "day" : "night";
    };

    const initialTheme = getStoredTheme() || detectSystemTheme();
    applyTheme(initialTheme);
})();
