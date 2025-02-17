// Dark Mode Toggle
function toggleDarkMode() {
    const body = document.body;
    body.classList.toggle("dark-mode");

    // Save user preference in localStorage
    const isDarkMode = body.classList.contains("dark-mode");
    localStorage.setItem("darkMode", isDarkMode);

    // Update the toggle switch state
    const toggle = document.getElementById("modeToggle");
    toggle.checked = isDarkMode;
}

// Check for saved user preference
function loadDarkModePreference() {
    const isDarkMode = localStorage.getItem("darkMode") === "true";
    if (isDarkMode) {
        document.body.classList.add("dark-mode");
        document.getElementById("modeToggle").checked = true;
    }
}

// Load preference on page load
loadDarkModePreference();
