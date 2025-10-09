check_symlink()
{
  SYMLINK="$INSTALL_DIR/lib/$1"
  EXPECTED_TARGET="$2"

  if test ! -L "$SYMLINK"; then
    echo "$SYMLINK should be a symlink"
    exit 1
  fi

  TARGET=$(readlink "$SYMLINK")
  if test ! -f $INSTALL_DIR/lib/$TARGET; then
    echo "$SYMLINK target $INSTALL_DIR/lib/$TARGET is missing!"
    exit 1
  else
    echo "$SYMLINK target $INSTALL_DIR/lib/$TARGET check ok"
  fi

  if [ "$TARGET" != "$EXPECTED_TARGET" ]; then
    echo "$SYMLINK should symlink to $EXPECTED_TARGET, not to $TARGET"
    exit 1
  else
    echo "$SYMLINK links to correct filename"
  fi
}
