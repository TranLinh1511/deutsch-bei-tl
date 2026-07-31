    tailwind.config = {
      darkMode: "class",
      theme: {
        extend: {
          fontFamily: {
            syne: ["Syne", "system-ui", "sans-serif"],
            mono: ['"DM Mono"', "monospace"],
          },
          colors: {
            bg: { DEFAULT: "#0d1117", 2: "#161b22", 3: "#1c2333" },
            border: { DEFAULT: "#30363d", 2: "#21262d" },
            accent: {
              blue: "#58a6ff",
              green: "#3fb950",
              red: "#f78166",
              purple: "#d2a8ff",
              gold: "#f0c000",
            },
            tx: { DEFAULT: "#e6edf3", 2: "#8b949e", 3: "#6e7681" },
          },
          keyframes: {
            flagPulse: {
              "0%,100%": { opacity: "1" },
              "50%": { opacity: "0.6" },
            },
            fadeSlideIn: {
              from: { opacity: "0", transform: "translateY(-6px)" },
              to: { opacity: "1", transform: "translateY(0)" },
            },
            smoothGlow: {
              "0%,100%": { boxShadow: "0 0 0 rgba(88, 166, 255, 0)" },
              "50%": { boxShadow: "0 0 12px rgba(88, 166, 255, 0.3)" },
            },
            slideInUp: {
              from: { opacity: "0", transform: "translateY(8px)" },
              to: { opacity: "1", transform: "translateY(0)" },
            },
            scaleIn: {
              from: { opacity: "0", transform: "scale(0.95)" },
              to: { opacity: "1", transform: "scale(1)" },
            },
          },
          animation: {
            flagPulse: "flagPulse 2s ease-in-out infinite",
            fadeSlideIn: "fadeSlideIn 0.25s ease",
            smoothGlow: "smoothGlow 3s ease-in-out infinite",
            slideInUp: "slideInUp 0.35s cubic-bezier(0.34, 1.56, 0.64, 1)",
            scaleIn: "scaleIn 0.3s cubic-bezier(0.34, 1.56, 0.64, 1)",
          },
        },
      },
    };
