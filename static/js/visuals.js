document.addEventListener("DOMContentLoaded", () => {
    const toggle = document.getElementById("literal-toggle");
    const label = document.getElementById("toggle-label");
    
    if (!toggle || !label) return; // safety check

    // Load saved state
    const saved = localStorage.getItem("strictToggle");
    if (saved !== null) {
        toggle.checked = saved === "true";
    }
    
    const updateLabel = () => {
        if (toggle.checked) {
            label.textContent = "LITERAL"; // strict search
            label.style.color = "var(--color-accent-b)";
            label.style.borderColor = "var(--color-accent-b)";
        } else {
            label.textContent = "NEURAL"; // semantic
            label.style.color = "var(--color-accent-r)";
            label.style.borderColor = "var(--color-accent-r)";
        }
        
        localStorage.setItem("strictToggle", toggle.checked);
    };
    
    toggle.addEventListener("change", updateLabel);

    updateLabel();
});
