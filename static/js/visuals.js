document.addEventListener("DOMContentLoaded", () => {
    const toggle = document.getElementById("literal-toggle");

    const saved = localStorage.getItem("searchMode");
    if (saved === "lit") {
        toggle.checked = true;
    }

    toggle.addEventListener("change", () => {
        localStorage.setItem(
            "searchMode",
            toggle.checked ? "lit" : "neu"
        );
    });
});