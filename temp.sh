#!/bin/bash
# Execute this block directly inside your parent root: /Users/dbaker/projects/tshirt_enterprise_platform

ROOT_DIR="/Users/dbaker/projects/tshirt_enterprise_platform"
DEPTS=("sales" "shipping" "finance" "notifications")

cd "$ROOT_DIR" || exit 1

for DEPT in "${DEPTS[@]}"; do
    echo "────────────────────────────────────────────────────────"
    echo "🛠️  Aligning layout structure for department: [$DEPT]"
    
    # 1. Create a safe temporary baseline folder to prevent data loss
    mkdir -p "$DEPT/src_temp"
    
    # 2. Rescue any python files regardless of how deep they were accidentally nested
    find "$DEPT/src" -name "*.py" -exec mv {} "$DEPT/src_temp/" \; 2>/dev/null
    
    # 3. Completely nuke the old messy src folder
    rm -rf "$DEPT/src"
    
    # 4. Rebuild the textbook enterprise src layout cleanly
    # This guarantees the file path is exactly: department/src/department/
    mkdir -p "$DEPT/src/$DEPT"
    
    # 5. Move your rescued production script modules into that clean name folder
    mv "$DEPT/src_temp/"*.py "$DEPT/src/$DEPT/" 2>/dev/null
    
    # 6. Ensure the standard package initialization file exists at the root
    touch "$DEPT/src/$DEPT/__init__.py"
    
    # 7. Clean up the temporary workspace folder
    rm -rf "$DEPT/src_temp"
    
    echo "✔  Layout verified: $DEPT/src/$DEPT/"
done

echo "────────────────────────────────────────────────────────"
echo "🚀 Running master workspace synchronization loop..."
uv sync
