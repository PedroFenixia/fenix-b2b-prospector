// Minimal JS - HTMX handles most interactivity
document.addEventListener('htmx:afterRequest', function(event) {
    // Scroll to top of results after search
    if (event.detail.target && event.detail.target.id === 'results') {
        event.detail.target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
});
