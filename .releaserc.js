module.exports = {
  branches: ["main"],
  plugins: [
    ["@semantic-release/commit-analyzer", {
      parserOpts: {
        // On fusionne le type et le ! dans le premier groupe de capture
        headerPattern: /^(\w*!?)(?:\((.*)\))?: (.*)$/,
        headerCorrespondence: ['type', 'scope', 'subject'],
      },
      releaseRules: [
        // On définit des règles explicites pour les types AVEC point d'exclamation
        { type: "feat!", release: "major" },
        { type: "fix!", release: "major" },
        { type: "chore!", release: "major" },
        // Règles standards pour le reste
        { type: "feat", release: "minor" },
        { type: "fix", release: "patch" },
        { type: "docs", release: "patch" }
      ]
    }],
    "@semantic-release/release-notes-generator",
    ["@semantic-release/exec", {
      prepareCmd: "sed -i 's/__version__ = \".*\"/__version__ = \"${nextRelease.version}\"/' src/radon/__init__.py"
    }],
    ["@semantic-release/git", {
      assets: ["src/radon/__init__.py"],
      message: "chore(release): ${nextRelease.version} [skip ci]"
    }],
    "@semantic-release/github"
  ]
};