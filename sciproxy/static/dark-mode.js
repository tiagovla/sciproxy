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
    const savedPreference = localStorage.getItem("darkMode");
    const isDarkMode = savedPreference === null || savedPreference === "true";
    document.body.classList.toggle("dark-mode", isDarkMode);
    document.getElementById("modeToggle").checked = isDarkMode;
}

// Load preference on page load
loadDarkModePreference();
