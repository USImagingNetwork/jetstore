package git

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// This package execute git command in the workspace directory

// Environment needed:
// WORKSPACE Workspace currently in use
// WORKSPACES_HOME Home dir of workspaces

type WorkspaceGit struct {
	WorkspaceName   string
	WorkspaceUri    string
	WorkspacesHome  string
	ActiveWorkspace string
}

func NewWorkspaceGit(workspaceName, workspaceUri string) *WorkspaceGit {
	return &WorkspaceGit{
		WorkspaceName:  workspaceName,
		WorkspaceUri:   workspaceUri,
		WorkspacesHome: os.Getenv("WORKSPACES_HOME"),
		ActiveWorkspace: os.Getenv("WORKSPACE"),
	}
}

func runShellCommand(dir, command string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", command)
	cmd.Dir = dir
	var b1 bytes.Buffer
	cmd.Stdout = &b1
	cmd.Stderr = &b1
	err := cmd.Run()
	if err != nil {
		err = fmt.Errorf("error while executing '%s' command (see log)", command)
		log.Printf("%v", err)
		return b1.String(), err
	}
	return b1.String(), nil
}

// Function to delete local workspace directory
func (wg *WorkspaceGit) DeleteWorkspace() error {
	if wg.WorkspaceName == wg.ActiveWorkspace {
		return fmt.Errorf("invaid request, cannot delete the active workspace")
	}
	log.Printf("Deleting local workspace '%s' directory", wg.WorkspaceName)
	err := exec.Command("rm", "--recursive", "--force", fmt.Sprintf("%s/%s", wg.WorkspacesHome, wg.WorkspaceName)).Run()
	if err != nil {
		return fmt.Errorf("while deleting the local workspace dir %s: %v", wg.WorkspaceName, err)
	}
	return nil
}

func (wg *WorkspaceGit) GetStatus() (string, error) {
	if wg.WorkspaceName == "" {
		return "", fmt.Errorf("error, must provide workspace_name")
	}
	workspacePath := fmt.Sprintf("%s/%s", wg.WorkspacesHome, wg.WorkspaceName)
	
	// First, check if workspace directory exists or not
	if _, err := os.Stat(workspacePath); os.IsNotExist(err) {
		return "removed", nil
 	}

	// Check if the local branch exists
	_, err := runShellCommand(workspacePath, fmt.Sprintf("git show-ref --verify --quiet refs/heads/%s", wg.WorkspaceName))
	if err != nil {
		// Local branch does not exist, must be a newly deployed container
		log.Printf("Branch '%s' does not exist in local repo %s", wg.WorkspaceName, workspacePath)
		return "removed", nil
	}

	// Check if user info exist in local repo
	result, err := runShellCommand(workspacePath, "git config --get user.email")
	if err != nil {
		// This is not expected
		return "", fmt.Errorf("error while trying to get local user info")
	}
	if len(result) == 0 {
		// Local user info does not exist, must be a newly deployed container
		log.Printf("Branch '%s' does not have user info in local repo %s", wg.WorkspaceName, workspacePath)
		return "removed", nil
	}

	// Check if workspace is the active workspace
	isActiveWorkspace := false
	if wg.WorkspaceName == wg.ActiveWorkspace {
		isActiveWorkspace = true
	}
	
	// Issue the git status command to see if workspace has modifications
	result, err = runShellCommand(workspacePath, "git status")
	if err != nil {
		return "", fmt.Errorf("error while executing 'git status' command: %v", err)
	}
	if strings.Contains(result, "nothing to commit, working tree clean") {
		if isActiveWorkspace {
			return "active", nil
		}
		return "no changes", nil
	}
	if isActiveWorkspace {
		return "active modified", nil
	}
	return "modified", nil
}

func (wg *WorkspaceGit) UpdateLocalWorkspace(userName, userEmail, gitUser, gitToken, wsPreviousName string) (string, error) {
	// Insert or update workspace entry in workspace_registry table:
	//	- If folder workspace_name in workspaces root does not exists, chechout workspace_uri in workspace_name
	//  - If user is renaming workspace_name, delete the old workspace folder under workspaces root
	//    Note: UI must provide old workspace name as 'previous.workspace_name' virtual column
	if wg.WorkspaceName == "" {
		return "", fmt.Errorf("error, must provide workspace_name")
	}
	workspacePath := fmt.Sprintf("%s/%s", wg.WorkspacesHome, wg.WorkspaceName)
	var buf strings.Builder

	// First, check if workspace directory exists or not
	if _, err := os.Stat(workspacePath); os.IsNotExist(err) {
		buf.WriteString("== Workspace directory does not exist, checking out workspace from git ==\n")
		gitRepo := strings.TrimPrefix(wg.WorkspaceUri, "https://")
		command := fmt.Sprintf("git clone --quiet 'https://%s:%s@%s' %s", gitUser, gitToken, gitRepo, wg.WorkspaceName)
		result, err := runShellCommand(wg.WorkspacesHome, command)
		buf.WriteString(result)
		if err != nil {
			return buf.String(), err
		}
		buf.WriteString("\n")
 	}

	// Check if user info exist in local repo
	result, err := runShellCommand(workspacePath, "git config --get user.email")
	buf.WriteString(result)
	if err != nil {
		// This is not expected
		return buf.String(), fmt.Errorf("error while trying to get local user info")
	}
	if len(result) == 0 {
		// Local user info does not exist, must be a newly deployed container
		buf.WriteString(fmt.Sprintf("Local repo '%s' does not have user info, configuring it.\n", workspacePath))
		command := fmt.Sprintf("git config user.email \"%s\"", userEmail)
		result, err = runShellCommand(workspacePath, command)
  	buf.WriteString(result)
		if err != nil {
			return buf.String(), err
		}
		buf.WriteString("\n")
		command = fmt.Sprintf("git config user.name \"%s\"", userName)
		result, err = runShellCommand(workspacePath, command)
  	buf.WriteString(result)
		if err != nil {
			return buf.String(), err
		}
	}
	buf.WriteString("\n")

	// Check if the local branch exists
	_, err = runShellCommand(workspacePath, fmt.Sprintf("git show-ref --verify --quiet refs/heads/%s", wg.WorkspaceName))
	buf.WriteString(result)
	if err != nil {
		// Local branch does not exist, must be a newly deployed container
		buf.WriteString(fmt.Sprintf("Branch '%s' does not exist in local repo %s\nCreating it...\n", wg.WorkspaceName, workspacePath))
		// git checkout -b <WorkspaceName>
		// git push -u origin HEAD
		command := fmt.Sprintf("git checkout -b %s", wg.WorkspaceName)
		result, err := runShellCommand(workspacePath, command)
		buf.WriteString(result)
		if err != nil {
			return buf.String(), err
		}
		buf.WriteString("\n")
		result, err = runShellCommand(workspacePath, "git push -u origin HEAD")
		buf.WriteString(result)
		if err != nil {
			return buf.String(), err
		}
	}
	return buf.String(), nil
}

func (wg *WorkspaceGit) CommitLocalWorkspace(gitUser, gitToken, wsCommitMessage string) (string, error) {
	// Commit and push workspace changes, git commands to execute:
	// git add -A
	// git commit -m '<message>'
	// git push 'https://<user>:<token>@<repo>' 
		if wg.WorkspaceName == "" {
		return "", fmt.Errorf("error, must provide workspace_name")
	}
	workspacePath := fmt.Sprintf("%s/%s", wg.WorkspacesHome, wg.WorkspaceName)
	var buf strings.Builder

	// Add changes to git index
	result, err := runShellCommand(workspacePath, "git add -A")
	buf.WriteString(result)
	if err != nil {
		return buf.String(), fmt.Errorf("error while trying to (git) add file contents to the index")
	}
	buf.WriteString("\n")

	// Commit changes
	if wsCommitMessage == "" {
		wsCommitMessage = "Changes from JetStore UI"
	}
	result, err = runShellCommand(workspacePath, fmt.Sprintf("git commit -m '%s'", wsCommitMessage))
	buf.WriteString(result)
	if err != nil {
		return buf.String(), fmt.Errorf("error while trying to (commit) record changes to the repository")
	}
	buf.WriteString("\n")

	gitRepo := strings.TrimPrefix(wg.WorkspaceUri, "https://")
	command := fmt.Sprintf("git push 'https://%s:%s@%s'", gitUser, gitToken, gitRepo)
	result, err = runShellCommand(workspacePath, command)
	buf.WriteString(result)
	if err != nil {
		return buf.String(), err
	}
	buf.WriteString("\nChanges pushed to repository\n")

	return buf.String(), nil
}

func (wg *WorkspaceGit) PullRemoteWorkspace(gitUser, gitToken string) (string, error) {
	// Pull workspace changes, git commands to execute:
	// git pull --rebase=false --no-commit 'https://<user>:<token>@<repo>' <WorkspaceName> 
		if wg.WorkspaceName == "" {
		return "", fmt.Errorf("error, must provide workspace_name")
	}
	workspacePath := fmt.Sprintf("%s/%s", wg.WorkspacesHome, wg.WorkspaceName)
	var buf strings.Builder

	gitRepo := strings.TrimPrefix(wg.WorkspaceUri, "https://")
	command := fmt.Sprintf("git pull --rebase=false --no-commit 'https://%s:%s@%s' %s", gitUser, gitToken, gitRepo, wg.WorkspaceName)
	result, err := runShellCommand(workspacePath, command)
	buf.WriteString(result)
	if err != nil {
		return buf.String(), err
	}
	buf.WriteString("\nChanges pulled from repository\n")

	return buf.String(), nil
}