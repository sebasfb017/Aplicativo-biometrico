self.addEventListener("install", function(event) {
    console.log("SW Installed");
});

self.addEventListener("fetch", function(event) {
    // Basic pass-through fetch
    event.respondWith(fetch(event.request));
});