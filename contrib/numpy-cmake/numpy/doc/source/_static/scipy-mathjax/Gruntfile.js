/*
MathJax-grunt-cleaner
=====================
A grunt file to reduce the footprint of a MathJax installation

Latest version at https://github.com/pkra/MathJax-grunt-cleaner

Copyright (c) 2014 Mathjax Consortium

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

module.exports = function(grunt) {
  "use strict";
  //   # Notes #
  //   NEVER remove:
  //
  //   * LICENSE -- the Apache license.
  //   * jax/element/mml -- this implements MathJax"s internal format. Keep either the packed or unpacked copy.
  //

  grunt.initConfig({
    pkg: grunt.file.readJSON("package.json"),
    clean: {
      //
      // ## Early choices
      // `unpacked` for development
      // ``packed` for production
      unpacked: [
        "unpacked"
      ],
      packed: [
        "config",
        "docs",
        "extensions",
        "jax",
        "localization",
        "MathJax.js"
      ],
      // If you don"t need combined configuration files or want to build your own:
      allConfigs: [
        "config",
        "unpacked/config"
      ],
      //
      // ## Choosing a font
      // See http://docs.mathjax.org/en/latest/font-support.html#font-configuration for background information
      //
      // 1. Remove font files and font data for fonts you won"t use.
      //    **IMPORTANT.** Make sure to prevent fallbacks and local fonts in your configuration!
      //
      //
      fontAsana: [
        "fonts/HTML-CSS/Asana-Math",
        "jax/output/HTML-CSS/fonts/Asana-Math",
        "unpacked/jax/output/HTML-CSS/fonts/Asana-Math",
        "jax/output/SVG/fonts/Asana-Math",
        "unpacked/jax/output/SVG/fonts/Asana-Math"
      ],
      fontGyrePagella: [
        "fonts/HTML-CSS/Gyre-Pagella",
        "jax/output/HTML-CSS/fonts/Gyre-Pagella",
        "unpacked/jax/output/HTML-CSS/fonts/Gyre-Pagella",
        "jax/output/SVG/fonts/Gyre-Pagella",
        "unpacked/jax/output/SVG/fonts/Gyre-Pagella"
      ],
      fontGyreTermes: [
        "fonts/HTML-CSS/Gyre-Termes",
        "jax/output/HTML-CSS/fonts/Gyre-Termes",
        "unpacked/jax/output/HTML-CSS/fonts/Gyre-Termes",
        "jax/output/SVG/fonts/Gyre-Termes",
        "unpacked/jax/output/SVG/fonts/Gyre-Termes"
      ],
      fontLatinModern: [
        "fonts/HTML-CSS/Latin-Modern",
        "jax/output/HTML-CSS/fonts/Latin-Modern",
        "unpacked/jax/output/HTML-CSS/fonts/Latin-Modern",
        "jax/output/SVG/fonts/Latin-Modern",
        "unpacked/jax/output/SVG/fonts/Latin-Modern"
      ],
      fontNeoEuler: [
        "fonts/HTML-CSS/Neo-Euler",
        "jax/output/HTML-CSS/fonts/Neo-Euler",
        "unpacked/jax/output/HTML-CSS/fonts/Neo-Euler",
        "jax/output/SVG/fonts/Neo-Euler",
        "unpacked/jax/output/SVG/fonts/Neo-Euler"
      ],
      fontStix: [
        "fonts/HTML-CSS/STIX",
        "jax/output/HTML-CSS/fonts/STIX",
        "unpacked/jax/output/HTML-CSS/fonts/STIX",
        "jax/output/SVG/fonts/STIX",
        "unpacked/jax/output/SVG/fonts/STIX"
      ],
      fontStixWeb: [
        "fonts/HTML-CSS/STIX-Web",
        "jax/output/HTML-CSS/fonts/STIX-Web",
        "unpacked/jax/output/HTML-CSS/fonts/STIX-Web",
        "jax/output/SVG/fonts/STIX-Web",
        "unpacked/jax/output/SVG/fonts/STIX-Web"
      ],
      fontTeX: [
        "fonts/HTML-CSS/TeX",
        "jax/output/HTML-CSS/fonts/TeX",
        "unpacked/jax/output/HTML-CSS/fonts/TeX",
        "jax/output/SVG/fonts/TeX",
        "unpacked/jax/output/SVG/fonts/TeX"
      ],
      //
      // Remove font formats
      // If you know you only need a specific format of your remaining fonts (e.g., woff), then you can delete the others.
      dropFonts: [ // if you use SVG output, you can drop all font formats (SVG output uses the data in `jax/output/SVG/fonts/...`)
        "fonts"
      ],
      eot: [
        "fonts/**/eot"
      ],
      otf: [
        "fonts/**/otf"
      ],
      png: [
        "fonts/**/png"
      ],
      svg: [ // **NOT** related to the SVG output!
        "fonts/**/svg"
      ],
      woff: [
        "fonts/**/woff"
      ],
      // ## Choose the input
      //    Remove input that you don"t need.
      //    **Note.** This includes combined configuration files.
      asciimathInput: [
        "config/AM*",
        "config/TeX-MML-AM*",
        "jax/input/AsciiMath",
        "unpacked/config/AM*",
        "unpacked/config/TeX-MML-AM*",
        "unpacked/jax/input/AsciiMath"
      ],
      mathmlInput: [
        "config/MML*",
        "config/TeX-MML*",
        "config/TeX-AMS-MML*",
        "jax/input/MathML",
        "unpacked/config/MathML*",
        "unpacked/jax/input/MathML"
      ],
      texInput: [
        "config/TeX*",
        "jax/input/TeX",
        "unpacked/config/TeX*",
        "unpacked/jax/input/TeX"
      ],
      // ## Extensions
      extensionsAsciimath: [
        "extensions/asciimath2jax.js",
        "unpacked/extensions/asciimath2jax.js"
      ],
      extensionsMathml: [
        "extensions/MathML",
        "extensions/mml2jax.js",
        "unpacked/extensions/MathML",
        "unpacked/extensions/mml2jax.js"
      ],
      extensionsTeX: [
        "extensions/TeX",
        "extensions/jsMath2jax.js",
        "extensions/tex2jax.js",
        "unpacked/extensions/TeX",
        "unpacked/extensions/jsMath2jax.js",
        "unpacked/extensions/tex2jax.js"
      ],
      extensionHtmlCss: [
        "extensions/HTML-CSS",
        "unpacked/extensions/HTML-CSS"
      ],
      // ## Choose Output
      htmlCssOutput: [
        "config/*HTMLorMML.js",
        "config/*HTMLorMML-full.js",
        "unpacked/config/*HTMLorMML.js",
        "unpacked/config/*HTMLorMML-full.js",
        "jax/output/HTML-CSS",
        "unpacked/jax/output/HTML-CSS"
      ],
      mathmlOutput: [
        "config/*HTMLorMML.js",
        "config/*HTMLorMML-full.js",
        "unpacked/config/*HTMLorMML.js",
        "unpacked/config/*HTMLorMML-full.js",
        "jax/output/NativeMML",
        "unpacked/jax/output/NativeMML"
      ],
      svgOutput: [
        "config/*SVG.js",
        "config/*SVG-full.js",
        "unpacked/config/*SVG.js",
        "unpacked/config/*SVG-full.js",
        "jax/output/SVG",
        "unpacked/jax/output/SVG"
      ],
      commonHtmlOutput: [
        "configs/*CHTML.js",
        "configs/*CHTML-full.js",
        "unpacked/config/*CHTML.js",
        "unpacked/configs/*CHTML-full.js",
        "jax/output/CommonHTML",
        "unpacked/jax/output/CommonHTML",
        "extensions/CHTML-preview.js",
        "unpacked/extensions/CHTML-preview.js"
      ],
      previewHtmlOutput: [
        "jax/output/PreviewHTML",
        "unpacked/jax/output/PreviewHTML",
        "extensions/fast-preview.js",
        "unpacked/extensions/fast-preview.js",
        "extensions/CHTML-preview.js",
        "unpacked/extensions/CHTML-preview.js"
      ],
      plainSourceOutput: [
        "jax/output/PlainSource",
        "unpacked/jax/output/PlainSource"
      ],
      //  ## Locales
      //  Removes all locale files. Change this as needed to keep your preferred language.
      //  **NOTE.** English strings are hardcoded.
      //  **NOTE.** If you fix the locale, drop the menu entry: http://docs.mathjax.org/en/latest/options/MathMenu.html#configure-mathmenu
      locales: [
        "localization",
        "unpacked/localization"
      ],
      // ## Misc.
      miscConfig: [
        "config/local",
        "unpacked/config/local",
        "config/Accessible-full.js",
        "unpacked/config/Accessible-full.js",
        "config/Accessible.js",
        "unpacked/config/Accessible.js",
        "config/default.js",
        "unpacked/config/default.js",
        "config/Safe.js",
        "unpacked/config/Safe.js"
      ],
      a11yExtensions: [
        "extensions/AssistiveMML.js",
        "unpacked/extensions/AssistiveMML.js"
      ],
      miscExtensions: [
        "extensions/FontWarnings.js",
        "extensions/HelpDialog.js",
        "extensions/MatchWebFonts.js",
        "extensions/MathEvents.js",
        "extensions/MathMenu.js",
        "extensions/MathZoom.js",
        "extensions/Safe.js",
        "extensions/CHTML-preview.js",
        //        "extensions/toMathML.js",  // only remove `toMathML.js` if you know exactly what you are doing.
        "unpacked/extensions/FontWarnings.js",
        "unpacked/extensions/HelpDialog.js",
        "unpacked/extensions/MatchWebFonts.js",
        "unpacked/extensions/MathEvents.js",
        "unpacked/extensions/MathMenu.js",
        "unpacked/extensions/MathZoom.js",
        "unpacked/extensions/Safe.js",
        "unpacked/extensions/CHTML-preview.js"
        //        "unpacked/extensions/toMathML.js",  // only remove `toMathML.js` if you know exactly what you are doing.
      ],
      images: [
        "images" // these are used in the menu. Removing them will give you 404 errors but nothing will break.
      ],
      notcode: [
        ".gitignore",
        "docs",
        "test",
        "CONTRIBUTING.md",
        "README-branch.txt",
        "README.md",
        "bower.json",
        "composer.json",
        ".npmignore",
        "package.json"
      ]
    },
    "regex-replace": {
      // disable image fonts in default HTML-CSS config
      noImageFont: {
        src: ['unpacked/jax/output/HTML-CSS/config.js'],
        actions: [
          {
            name: 'nullImageFont',
            search: /imageFont:[^,]+,/,
            replace: 'imageFont: null,',
          }
        ]
      }
    }
  });

  grunt.loadNpmTasks("grunt-contrib-clean");
  grunt.loadNpmTasks('grunt-regex-replace');

  grunt.registerTask("component", [
    // components-mathjax excludes only PNG fonts
    "regex-replace:noImageFont",
    "clean:png",
  ]);

  grunt.registerTask("template", [
    // **Notes** on the template. When instructions say "Pick one", this means commenting out one item (so that it"s not cleaned).
    //
    //      Early choices.
    "clean:unpacked",
    "clean:packed", // pick one -- packed for production, unpacked for development.
    "clean:allConfigs", // if you do not need any combined configuration files.
    //      Fonts. Pick at least one! Check notes above on configurations.
    "clean:fontAsana",
    "clean:fontGyrePagella",
    "clean:fontGyreTermes",
    "clean:fontLatinModern",
    "clean:fontNeoEuler",
    "clean:fontStix",
    "clean:fontStixWeb",
    "clean:fontTeX",
    //      Font formats. Pick at least one (unless you use SVG output; then clean all).
    "clean:dropFonts", // when using SVG output
    "clean:eot",
    "clean:otf",
    "clean:png",
    "clean:svg",
    "clean:woff",
    //      Input. Pick at least one.
    "clean:asciimathInput",
    "clean:mathmlInput",
    "clean:texInput",
    //       Output
    "clean:htmlCssOutput",
    "clean:mathmlOutput",
    "clean:svgOutput",
    // Extensions. You probably want to leave the set matching your choices.
    "clean:extensionsAsciimath",
    "clean:extensionsMathml",
    "clean:extensionsTeX",
    "clean:extensionHtmlCss",
    // Other items
    "clean:locales",
    "clean:miscConfig",
    //        "clean:miscExtensions", // you probably want that
    "clean:images",
    "clean:notcode"
  ]);
  grunt.registerTask("TeX_SVG", [
    //      Early choices.
    //        "clean:unpacked",
    "clean:packed", // pick one -- packed for production, unpacked for development.
    "clean:allConfigs", // if you do not need any combined configuration files.
    //      Fonts. Pick at least one! Check notes above on configurations.
    "clean:fontAsana",
    "clean:fontGyrePagella",
    "clean:fontGyreTermes",
    "clean:fontLatinModern",
    "clean:fontNeoEuler",
    "clean:fontStix",
    "clean:fontStixWeb",
    //        "clean:fontTeX",
    //      Font formats. Pick at least one (unless you use SVG output; then clean all).
    "clean:dropFonts", // when using SVG output
    "clean:eot",
    "clean:otf",
    "clean:png",
    "clean:svg",
    "clean:woff",
    //      Input. Pick at least one.
    "clean:asciimathInput",
    "clean:mathmlInput",
    //        "clean:texInput",
    //       Output
    "clean:htmlCssOutput",
    "clean:mathmlOutput",
    //        "clean:svgOutput",
    // Extensions. You probably want to leave the set matching your choices.
    "clean:extensionsAsciimath",
    "clean:extensionsMathml",
    //        "clean:extensionsTeX",
    "clean:extensionHtmlCss",
    // Other items
    "clean:locales",
    "clean:miscConfig",
    //        "clean:miscExtensions",
    "clean:images",
    "clean:notcode"
  ]);
  grunt.registerTask("MML_SVG_TeX", [
    //      Early choices.
    "clean:unpacked",
    //        "clean:packed", // pick one -- packed for production, unpacked for development.
    "clean:allConfigs", // if you do not need any combined configuration files.
    //      Fonts. Pick at least one! Check notes above on configurations.
    "clean:fontAsana",
    "clean:fontGyrePagella",
    "clean:fontGyreTermes",
    "clean:fontLatinModern",
    "clean:fontNeoEuler",
    "clean:fontStix",
    "clean:fontStixWeb",
    //        "clean:fontTeX",
    //      Font formats. Pick at least one (unless you use SVG output; then clean all).
    "clean:dropFonts", // when using SVG output
    "clean:eot",
    "clean:otf",
    "clean:png",
    "clean:svg",
    "clean:woff",
    //      Input. Pick at least one.
    "clean:asciimathInput",
    //        "clean:mathmlInput",
    "clean:texInput",
    //       Output
    "clean:htmlCssOutput",
    "clean:mathmlOutput",
    //        "clean:svgOutput",
    // Extensions. You probably want to leave the set matching your choices.
    "clean:extensionsAsciimath",
    //        "clean:extensionsMathml",
    "clean:extensionsTeX",
    "clean:extensionHtmlCss",
    // Other items
    "clean:locales",
    "clean:miscConfig",
    //        "clean:miscExtensions", // you probably want that
    "clean:images",
    "clean:notcode"
  ]);
  grunt.registerTask("mjNode", [
    "clean:packed",
    "clean:allConfigs",
    "clean:dropFonts",
    "clean:htmlCssOutput",
    "clean:locales",
    "clean:miscConfig",
    "clean:images",
    "clean:notcode",
    "clean:miscExtensions"
  ]);
};
